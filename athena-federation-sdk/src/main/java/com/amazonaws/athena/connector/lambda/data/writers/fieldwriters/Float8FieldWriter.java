 /*-
  * #%L
  * Amazon Athena Query Federation SDK
  * %%
  * Copyright (C) 2019 Amazon Web Services
  * %%
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * #L%
  */
 package com.amazonaws.athena.connector.lambda.data.writers.fieldwriters;

 import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
 import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
 import org.apache.arrow.vector.Float8Vector;
 import org.apache.arrow.vector.holders.NullableFloat8Holder;

 public class Float8FieldWriter
         implements FieldWriter
 {
     private final NullableFloat8Holder holder = new NullableFloat8Holder();
     private final Float8Extractor extractor;
     private final Float8Vector vector;
     private final ConstraintApplier constraint;

     public Float8FieldWriter(Float8Extractor extractor, Float8Vector vector, ConstraintProjector rawConstraint)
     {
         this.extractor = extractor;
         this.vector = vector;
         if (rawConstraint != null) {
             constraint = (NullableFloat8Holder value) -> rawConstraint.apply(value.isSet == 0 ? null : value.value);
         }
         else {
             constraint = (NullableFloat8Holder value) -> true;
         }
     }

     @Override
     public boolean write(Object context, int rowNum)
     {
         extractor.extract(context, rowNum, holder);
         vector.setSafe(rowNum, holder);
         return constraint.apply(holder);
     }

     private interface ConstraintApplier
     {
         boolean apply(NullableFloat8Holder value);
     }
 }
