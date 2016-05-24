/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.module.extension.internal.metadata;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import org.mule.functional.junit4.ExtensionFunctionalTestCase;
import org.mule.metadata.api.ClassTypeLoader;
import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.metadata.ComponentId;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataKeyBuilder;
import org.mule.runtime.api.metadata.MetadataManager;
import org.mule.runtime.api.metadata.descriptor.ComponentMetadataDescriptor;
import org.mule.runtime.api.metadata.descriptor.OutputMetadataDescriptor;
import org.mule.runtime.api.metadata.descriptor.TypeMetadataDescriptor;
import org.mule.runtime.api.metadata.resolving.FailureCode;
import org.mule.runtime.api.metadata.resolving.MetadataFailure;
import org.mule.runtime.api.metadata.resolving.MetadataResult;
import org.mule.runtime.core.api.MuleEvent;
import org.mule.runtime.core.internal.metadata.MuleMetadataManager;
import org.mule.runtime.extension.api.introspection.metadata.NullMetadataKey;
import org.mule.runtime.module.extension.internal.util.ExtensionsTestUtils;
import org.mule.test.metadata.extension.MetadataConnection;
import org.mule.test.metadata.extension.MetadataExtension;
import org.mule.test.metadata.extension.resolver.TestMetadataResolverUtils;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;


public abstract class MetadataExtensionFunctionalTestCase extends ExtensionFunctionalTestCase
{

    protected static final String FIRST_PROCESSOR_INDEX = "0";

    protected static final String METADATA_TEST = "metadata-tests.xml";

    protected static final String METADATA_TEST_STATIC_NO_REF_CONFIGURATION = "metadata-tests-static-no-ref-configuration.xml";
    protected static final String METADATA_TEST_DYNAMIC_NO_REF_CONFIGURATION = "metadata-tests-dynamic-no-ref-configuration.xml";
    protected static final String METADATA_TEST_DYNAMIC_IMPLICIT_CONFIGURATION = "metadata-tests-dynamic-implicit-configuration.xml";

    protected static final String CONTENT_METADATA_WITH_KEY_ID = "contentMetadataWithKeyId";
    protected static final String OUTPUT_METADATA_WITH_KEY_ID = "outputMetadataWithKeyId";
    protected static final String CONTENT_AND_OUTPUT_METADATA_WITH_KEY_ID = "contentAndOutputMetadataWithKeyId";
    protected static final String OUTPUT_ONLY_WITHOUT_CONTENT_PARAM = "outputOnlyWithoutContentParam";
    protected static final String CONTENT_ONLY_IGNORES_OUTPUT = "contentOnlyIgnoresOutput";
    protected static final String CONTENT_METADATA_WITHOUT_KEY_ID = "contentMetadataWithoutKeyId";
    protected static final String OUTPUT_METADATA_WITHOUT_KEY_PARAM = "outputMetadataWithoutKeyId";
    protected static final String CONTENT_AND_OUTPUT_METADATA_WITHOUT_KEY_ID = "contentAndOutputMetadataWithoutKeyId";
    protected static final String CONTENT_METADATA_WITHOUT_KEYS_WITH_KEY_ID = "contentMetadataWithoutKeysWithKeyId";
    protected static final String OUTPUT_METADATA_WITHOUT_KEYS_WITH_KEY_ID = "outputMetadataWithoutKeysWithKeyId";
    protected static final String CONTENT_AND_OUTPUT_CACHE_RESOLVER = "contentAndOutputWithCacheResolver";
    protected static final String CONTENT_AND_OUTPUT_CACHE_RESOLVER_WITH_ALTERNATIVE_CONFIG = "contentAndOutputWithCacheResolverWithSpecificConfig";
    protected static final String CONTENT_ONLY_CACHE_RESOLVER = "contentOnlyCacheResolver";
    protected static final String OUTPUT_AND_METADATA_KEY_CACHE_RESOLVER = "outputAndMetadataKeyCacheResolver";
    protected static final String SOURCE_METADATA = "sourceMetadata";
    protected static final String SOURCE_METADATA_WITH_MULTILEVEL = "sourceMetadataWithMultilevel";
    protected static final String SHOULD_INHERIT_OPERATION_RESOLVERS = "shouldInheritOperationResolvers";
    protected static final String SHOULD_INHERIT_EXTENSION_RESOLVERS = "shouldInheritExtensionResolvers";
    protected static final String SHOULD_INHERIT_OPERATION_PARENT_RESOLVERS = "shouldInheritOperationParentResolvers";
    protected static final String SIMPLE_MULTILEVEL_KEY_RESOLVER = "simpleMultiLevelKeyResolver";
    protected static final String TYPE_WITH_DECLARED_SUBTYPES_METADATA = "typeWithDeclaredSubtypesMetadata";
    protected static final String RESOLVER_WITH_DYNAMIC_CONFIG = "resolverWithDynamicConfig";
    protected static final String RESOLVER_WITH_IMPLICIT_DYNAMIC_CONFIG = "resolverWithImplicitDynamicConfig";

    protected final NullMetadataKey nullMetadataKey = new NullMetadataKey();
    protected MetadataType personType;

    protected MetadataKey personKey;
    protected ComponentId componentId;
    protected MuleEvent event;
    protected MetadataManager metadataManager;
    protected ClassTypeLoader typeLoader;

    @Override
    protected Class<?>[] getAnnotatedExtensionClasses()
    {
        return new Class<?>[] {MetadataExtension.class};
    }

    @Before
    public void setContext() throws Exception
    {
        event = getTestEvent("");
        metadataManager = muleContext.getRegistry().lookupObject(MuleMetadataManager.class);
    }

    @Before
    public void setMetadata() throws Exception
    {
        typeLoader = ExtensionsTestUtils.TYPE_LOADER;
        personKey = MetadataKeyBuilder.newKey(MetadataConnection.PERSON).build();
        personType = TestMetadataResolverUtils.getMetadata(personKey.getId());
    }

    protected ComponentMetadataDescriptor getComponentDynamicMetadata()
    {
        return getComponentDynamicMetadata(personKey);
    }

    protected ComponentMetadataDescriptor getComponentDynamicMetadata(MetadataKey key)
    {
        MetadataResult<ComponentMetadataDescriptor> componentMetadata = metadataManager.getMetadata(componentId, key);
        assertThat(componentMetadata.isSuccess(), is(true));
        return componentMetadata.get();
    }

    protected ComponentMetadataDescriptor getComponentStaticMetadata()
    {
        MetadataResult<ComponentMetadataDescriptor> componentMetadata = metadataManager.getMetadata(componentId);
        assertThat(componentMetadata.isSuccess(), is(true));

        return componentMetadata.get();
    }

    protected void assertFailure(MetadataResult<?> result, String msgContains, FailureCode failureCode, String traceContains) throws IOException
    {
        assertThat(result.isSuccess(), is(false));
        MetadataFailure metadataFailure = result.getFailure().get();
        assertThat(metadataFailure.getMessage(), metadataFailure.getFailureCode(), is(failureCode));

        if (!StringUtils.isBlank(msgContains))
        {
            assertThat(metadataFailure.getMessage(), containsString(msgContains));
        }

        if (!StringUtils.isBlank(traceContains))
        {
            assertThat(metadataFailure.getReason(), containsString(traceContains));
        }
    }

    protected void assertExpectedOutput(OutputMetadataDescriptor outputDescriptor, Type payloadType, Type attributesType) throws IOException
    {
        assertExpectedType(outputDescriptor.getPayloadMetadata(), "Message.Payload", payloadType);
        assertExpectedType(outputDescriptor.getAttributesMetadata(), "Message.Attributes", attributesType);
    }

    protected void assertExpectedOutput(OutputMetadataDescriptor outputDescriptor, MetadataType payloadType, Type attributesType) throws IOException
    {
        assertExpectedType(outputDescriptor.getPayloadMetadata(), "Message.Payload", payloadType);
        assertExpectedType(outputDescriptor.getAttributesMetadata(), "Message.Attributes", attributesType);
    }

    protected void assertExpectedType(TypeMetadataDescriptor param, String name, Type type) throws IOException
    {
        if (!StringUtils.isBlank(name))
        {
            assertThat(param.getName(), is(name));
        }
        assertThat(param.getType(), is(typeLoader.load(type)));
    }

    protected void assertExpectedType(TypeMetadataDescriptor param, String name, MetadataType type) throws IOException
    {
        if (!StringUtils.isBlank(name))
        {
            assertThat(param.getName(), is(name));
        }
        assertThat(param.getType(), is(type));
    }
}
