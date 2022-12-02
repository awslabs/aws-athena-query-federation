/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

public class StubbingCursor<T>
        implements MongoCursor<T>
{
    private Iterator<T> values;

    public StubbingCursor(Iterator<T> result)
    {
        this.values = result;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean hasNext()
    {
        return values.hasNext();
    }

    @Override
    public T next()
    {
        return values.next();
    }

    @Override
    public T tryNext()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerCursor getServerCursor()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerAddress getServerAddress()
    {
        throw new UnsupportedOperationException();
    }

    public static <X> MongoIterable<X> iterate(Collection<X> result)
    {
        return new MongoIterable<X>()
        {
            @Override
            public MongoCursor<X> iterator()
            {
                return new StubbingCursor<>(result.iterator());
            }

            @Override
            public MongoCursor<X> cursor()
            {
                return iterator();
            }

            @Override
            public X first()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <U> MongoIterable<U> map(Function<X, U> function)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void forEach(Block<? super X> block)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <A extends Collection<? super X>> A into(A objects)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public MongoIterable<X> batchSize(int i)
            {
                return this;
            }
        };
    }
}
