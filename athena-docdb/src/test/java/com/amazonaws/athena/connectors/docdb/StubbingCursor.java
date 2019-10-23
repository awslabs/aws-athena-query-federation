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
