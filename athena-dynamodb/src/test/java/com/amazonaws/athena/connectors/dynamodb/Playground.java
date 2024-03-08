package com.amazonaws.athena.connectors.dynamodb;

import org.testng.annotations.Test;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.EnhancedAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.ListAttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Playground {

    @Test
    public void ListOfAttribute(){
        AttributeValue attributeValue = AttributeValue.builder()
                .l(AttributeValue.builder().s("String1").build(),
                        AttributeValue.builder().s("String2").build(),
                        AttributeValue.builder().s("String3").build())
                .build();
        EnhancedAttributeValue enhancedAttributeValue = EnhancedAttributeValue.fromAttributeValue(attributeValue);

//        assertTrue(enhancedAttributeValue.isListOfAttributeValues());
//        ListAttributeConverter converter = ListAttributeConverter.builder(EnhancedType.listOf(String.class)).build();
//        System.out.println(converter.transformTo(dynamoDbList));
//        assertEquals(converter.transformTo(dynamoDbList).size(),3);
        // Create a ListAttributeConverter for a list of strings
        // Custom collection constructor
        Supplier<ArrayList<String>> collectionConstructor = ArrayList::new;

        // Create a ListAttributeConverter with a custom collection constructor
        ListAttributeConverter<List<String>> converter = ListAttributeConverter.builder(EnhancedType.listOf(String.class))
                .collectionConstructor(collectionConstructor)
                .build();


        // Convert the DynamoDB AttributeValue to a Java List
        List<String> javaList = converter.transformTo(attributeValue);

        // Output the Java list
        System.out.println(javaList);
    }
}
