/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.server.netty

import io.ktor.application.*
import io.ktor.server.engine.*
import io.ktor.util.*
import io.ktor.util.pipeline.*
import io.netty.bootstrap.*
import io.netty.channel.*
import io.netty.channel.epoll.*
import io.netty.channel.kqueue.*
import io.netty.channel.nio.*
import io.netty.channel.socket.*
import io.netty.channel.socket.nio.*
import io.netty.handler.codec.http.*
import io.netty.util.concurrent.*
import kotlinx.coroutines.*
import java.lang.reflect.*
import java.util.concurrent.*
import kotlin.reflect.*

/**
 * [ApplicationEngine] implementation for running in a standalone Netty
 */
public class NettyApplicationEngine(
    environment: ApplicationEngineEnvironment,
    configure: Configuration.() -> Unit = {}
) :
    BaseApplicationEngine(environment) {

    /**
     * Configuration for the [NettyApplicationEngine]
     */
    public class Configuration : BaseApplicationEngine.Configuration() {
        /**
         * Size of the queue to store [ApplicationCall] instances that cannot be immediately processed
         */
        public var requestQueueLimit: Int = 16

        /**
         * Number of concurrently running requests from the same http pipeline
         */
        public var runningLimit: Int = 10

        /**
         * Do not create separate call event group and reuse worker group for processing calls
         */
        public var shareWorkGroup: Boolean = false

        /**
         * User-provided function to configure Netty's [ServerBootstrap]
         */
        public var configureBootstrap: ServerBootstrap.() -> Unit = {}

        /**
         * Timeout in seconds for sending responses to client
         */
        public var responseWriteTimeoutSeconds: Int = 10

        /**
         * Timeout in seconds for reading requests from client, "0" is infinite.
         */
        public var requestReadTimeoutSeconds: Int = 0

        /**
         * If set to `true`, enables TCP keep alive for connections so all
         * dead client connections will be discarded.
         * The timeout period is configured by the system so configure
         * your host accordingly.
         */
        public var tcpKeepAlive: Boolean = false

        /**
         * User-provided function to configure Netty's [HttpServerCodec]
         */
        public var httpServerCodec: () -> HttpServerCodec = ::HttpServerCodec
    }

    private val configuration = Configuration().apply(configure)

    /**
     * [EventLoopGroupProxy] for accepting connections
     */
    private val connectionEventGroup: EventLoopGroupProxy by lazy {
        EventLoopGroupProxy.create(configuration.connectionGroupSize)
    }

    /**
     * [EventLoopGroupProxy] for processing incoming requests and doing engine's internal work
     */
    private val workerEventGroup: EventLoopGroupProxy by lazy {
        if (configuration.shareWorkGroup) {
            EventLoopGroupProxy.create(configuration.workerGroupSize + configuration.callGroupSize)
        } else {
            EventLoopGroupProxy.create(configuration.workerGroupSize)
        }
    }

    /**
     * [EventLoopGroupProxy] for processing [ApplicationCall] instances
     */
    private val callEventGroup: EventLoopGroupProxy by lazy {
        if (configuration.shareWorkGroup) {
            workerEventGroup
        } else {
            EventLoopGroupProxy.create(configuration.callGroupSize)
        }
    }

    private val dispatcherWithShutdown: DispatcherWithShutdown by lazy {
        DispatcherWithShutdown(NettyDispatcher)
    }

    private val engineDispatcherWithShutdown by lazy {
        DispatcherWithShutdown(workerEventGroup.asCoroutineDispatcher())
    }

    private var cancellationDeferred: CompletableJob? = null

    private var channels: List<Channel>? = null
    private val bootstraps: List<ServerBootstrap> by lazy {
        environment.connectors.map { connector ->
            ServerBootstrap().apply {
                configuration.configureBootstrap(this)

                val conf = config()
                if (conf.group() == null && conf.childGroup() == null) {
                    group(connectionEventGroup, workerEventGroup)
                }

                channel(connectionEventGroup.channel.java)
                childHandler(
                    NettyChannelInitializer(
                        pipeline, environment,
                        callEventGroup,
                        engineDispatcherWithShutdown,
                        environment.parentCoroutineContext + dispatcherWithShutdown,
                        connector,
                        configuration.requestQueueLimit,
                        configuration.runningLimit,
                        configuration.responseWriteTimeoutSeconds,
                        configuration.requestReadTimeoutSeconds,
                        configuration.httpServerCodec
                    )
                )
                if (configuration.tcpKeepAlive) {
                    option(NioChannelOption.SO_KEEPALIVE, true)
                }
            }
        }
    }

    init {
        val afterCall = PipelinePhase("After")
        pipeline.insertPhaseAfter(EnginePipeline.Call, afterCall)
        pipeline.intercept(afterCall) {
            (call as? NettyApplicationCall)?.finish()
        }
    }

    override fun start(wait: Boolean): NettyApplicationEngine {
        environment.start()

        channels = bootstraps.zip(environment.connectors)
            .map { it.first.bind(it.second.host, it.second.port) }
            .map { it.sync().channel() }

        cancellationDeferred = stopServerOnCancellation()

        if (wait) {
            channels?.map { it.closeFuture() }?.forEach { it.sync() }
            stop(1, 5, TimeUnit.SECONDS)
        }
        return this
    }

    override fun stop(gracePeriodMillis: Long, timeoutMillis: Long) {
        cancellationDeferred?.complete()
        environment.monitor.raise(ApplicationStopPreparing, environment)
        val channelFutures = channels?.mapNotNull { if (it.isOpen) it.close() else null }.orEmpty()

        dispatcherWithShutdown.prepareShutdown()
        engineDispatcherWithShutdown.prepareShutdown()
        try {
            val shutdownConnections =
                connectionEventGroup.shutdownGracefully(gracePeriodMillis, timeoutMillis, TimeUnit.MILLISECONDS)
            shutdownConnections.await()

            val shutdownWorkers =
                workerEventGroup.shutdownGracefully(gracePeriodMillis, timeoutMillis, TimeUnit.MILLISECONDS)
            if (configuration.shareWorkGroup) {
                shutdownWorkers.await()
            } else {
                val shutdownCall =
                    callEventGroup.shutdownGracefully(gracePeriodMillis, timeoutMillis, TimeUnit.MILLISECONDS)
                shutdownWorkers.await()
                shutdownCall.await()
            }

            environment.stop()
        } finally {
            dispatcherWithShutdown.completeShutdown()
            engineDispatcherWithShutdown.completeShutdown()

            channelFutures.forEach { it.sync() }
        }
    }

    override fun toString(): String {
        return "Netty($environment)"
    }
}

/**
 * Transparently allows for the creation of [EventLoopGroup]'s utilising the optimal implementation for
 * a given operating system, subject to availability, or falling back to [NioEventLoopGroup] if none is available.
 */
public class EventLoopGroupProxy(public val channel: KClass<out ServerSocketChannel>, group: EventLoopGroup) :
    EventLoopGroup by group {

    public companion object {

        public fun create(parallelism: Int): EventLoopGroupProxy {
            val defaultFactory = DefaultThreadFactory(EventLoopGroupProxy::class.java)

            val factory = ThreadFactory { runnable ->
                defaultFactory.newThread {
                    markParkingProhibited()
                    runnable.run()
                }
            }

            return when {
                KQueue.isAvailable() -> EventLoopGroupProxy(
                    KQueueServerSocketChannel::class,
                    KQueueEventLoopGroup(parallelism, factory)
                )
                Epoll.isAvailable() -> EventLoopGroupProxy(
                    EpollServerSocketChannel::class,
                    EpollEventLoopGroup(parallelism, factory)
                )
                else -> EventLoopGroupProxy(
                    NioServerSocketChannel::class,
                    NioEventLoopGroup(parallelism, factory)
                )
            }
        }

        private val prohibitParkingFunction: Method? by lazy {
            try {
                Class.forName("io.ktor.utils.io.jvm.javaio.PollersKt")
                    .getMethod("prohibitParking")
            } catch (cause: Throwable) {
                null
            }
        }

        private fun markParkingProhibited() {
            try {
                prohibitParkingFunction?.invoke(null)
            } catch (cause: Throwable) {
            }
        }
    }
}
