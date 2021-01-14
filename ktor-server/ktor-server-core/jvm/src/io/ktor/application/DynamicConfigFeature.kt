/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.application

import io.ktor.routing.*
import io.ktor.util.*
import io.ktor.util.pipeline.*

/**
 * Gets feature instance for this pipeline, or fails with [MissingApplicationFeatureException] if the feature is not installed
 * @throws MissingApplicationFeatureException
 * @param feature application feature to lookup
 * @return an instance of feature
 */
public fun <A : Pipeline<*, ApplicationCall>, B : Any, F : Any> A.feature(feature: DynamicConfigFeature<A, B, F>): F {
    return findFeatureInRoute(feature) ?: throw MissingApplicationFeatureException(feature.key)
}

public interface DynamicConfigFeature<in TPipeline : Pipeline<*, ApplicationCall>, TConfiguration : Any, TFeature : Any> :
    ApplicationFeature<TPipeline, TConfiguration, TFeature> {

    public val configKey: AttributeKey<TConfiguration.() -> Unit>
        get() = AttributeKey("${key.name}_configBuilder")

    @Deprecated(
        "This feature can change it's configurations by calling `config` function in routing. " +
            "To get latest config please use `getConfiguration` function inside call interceptor.",
        replaceWith = ReplaceWith("install")
    )
    public override fun install(pipeline: TPipeline, configure: TConfiguration.() -> Unit): TFeature {
        return install(pipeline)
    }

    public fun install(pipeline: TPipeline): TFeature

    public val PipelineContext<*, ApplicationCall>.configurationBlock: (TConfiguration.() -> Unit)
        get() = call.attributes[configKey]
}

/**
 * Installs [feature] into this pipeline, if it is not yet installed
 */
public fun <P : Pipeline<*, ApplicationCall>, B : Any, F : Any> P.install(
    feature: DynamicConfigFeature<P, B, F>,
    configure: B.() -> Unit = {}
): F {
    intercept(ApplicationCallPipeline.Setup) {
        call.attributes.put(feature.configKey, configure)
    }

    val installedFeature = findFeatureInRoute(feature)
    if (installedFeature != null) {
        return installedFeature
    }

    // dynamic feature needs to be installed into routing, because only routing will have all interceptors
    @Suppress("UNCHECKED_CAST")
    val installPipeline = when (this) {
        is Application -> routing {} as P
        else -> this
    }
    val installed = feature.install(installPipeline)
    val registry = installPipeline.attributes.computeIfAbsent(featureRegistryKey) { Attributes(true) }
    registry.put(feature.key, installed)
    //environment.log.trace("`${feature.name}` feature was installed successfully.")
    return installed
}

private fun <P : Pipeline<*, ApplicationCall>, B : Any, F : Any> P.findFeatureInRoute(
    feature: DynamicConfigFeature<P, B, F>
): F? {
    var current: Route? = this as? Route
    while (current != null) {
        val registry = current.attributes.computeIfAbsent(featureRegistryKey) { Attributes(true) }
        val installedFeature = registry.getOrNull(feature.key)
        if (installedFeature != null) return installedFeature
        current = current.parent
    }
    return null
}
