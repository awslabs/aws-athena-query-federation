/*-
 * #%L
 * Athena MSK Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.athena.connectors.msk.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.util.Objects.requireNonNull;

// Make it extendable so that each of the connector can implement their own field extractor
public class TrinoRecordSet
        implements Iterable<TrinoRecord>
{
    public static final int DEFAULT_PRECISION = 5;

    private final List<TrinoRecord> records;

    public TrinoRecordSet(List<TrinoRecord> records)
    {
        requireNonNull(records, "Records list was null");
        List<TrinoRecord> convertedRecords = records.stream()
                .map(this::convertToTypes)
                .collect(Collectors.toList());
        this.records = ImmutableList.copyOf(convertedRecords);
    }

    @Override
    public Iterator<TrinoRecord> iterator()
    {
        return records.iterator();
    }

    public static Builder resultBuilder(ConnectorSession session, Iterable<? extends Type> types)
    {
        return new Builder(session, ImmutableList.copyOf(types));
    }

    public static class Builder
    {
        private final ConnectorSession session;
        private final List<Type> types;
        private final ImmutableList.Builder<TrinoRecord> rows = ImmutableList.builder();

        Builder(ConnectorSession session, List<Type> types)
        {
            this.session = session;
            this.types = ImmutableList.copyOf(types);
        }

        public synchronized Builder rows(List<TrinoRecord> rows)
        {
            this.rows.addAll(rows);
            return this;
        }

        public synchronized Builder row(Object... values)
        {
            rows.add(new TrinoRecord(DEFAULT_PRECISION, values));
            return this;
        }

        public synchronized Builder rows(Object[][] rows)
        {
            for (Object[] row : rows) {
                row(row);
            }
            return this;
        }

        public synchronized Builder pages(Iterable<Page> pages)
        {
            for (Page page : pages) {
                this.page(page);
            }
            return this;
        }

        public synchronized Builder page(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session, block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new TrinoRecord(DEFAULT_PRECISION, values));
            }
            return this;
        }

        public synchronized TrinoRecordSet build()
        {
            return new TrinoRecordSet(rows.build());
        }
    }

    public boolean isEmpty()
    {
        return (records == null || records.isEmpty());
    }

    public TrinoRecord get(int index)
    {
        return records.get(index);
    }

    // helpers
    private TrinoRecord convertToTypes(TrinoRecord record)
    {
        List<Object> convertedValues = new ArrayList<>();
        for (int field = 0; field < record.getFieldCount(); field++) {
            Object trinoValue = record.getField(field);
            Object convertedValue;
            if (trinoValue instanceof SqlDate) {
                convertedValue = LocalDate.ofEpochDay(((SqlDate) trinoValue).getDays());
            }
            else if (trinoValue instanceof SqlTime) {
                convertedValue = DateTimeFormatter.ISO_LOCAL_TIME.parse(trinoValue.toString(), LocalTime::from);
            }
            else if (trinoValue instanceof SqlTimeWithTimeZone) {
                long nanos = roundDiv(((SqlTimeWithTimeZone) trinoValue).getPicos(), PICOSECONDS_PER_NANOSECOND);
                int offsetMinutes = ((SqlTimeWithTimeZone) trinoValue).getOffsetMinutes();
                convertedValue = OffsetTime.of(LocalTime.ofNanoOfDay(nanos), ZoneOffset.ofTotalSeconds(offsetMinutes * 60));
            }
            else if (trinoValue instanceof SqlTimestamp) {
                convertedValue = ((SqlTimestamp) trinoValue).toLocalDateTime();
            }
            else if (trinoValue instanceof SqlTimestampWithTimeZone) {
                convertedValue = ((SqlTimestampWithTimeZone) trinoValue).toZonedDateTime();
            }
            else if (trinoValue instanceof SqlDecimal) {
                convertedValue = ((SqlDecimal) trinoValue).toBigDecimal();
            }
            else {
                convertedValue = trinoValue;
            }
            convertedValues.add(convertedValue);
        }
        return new TrinoRecord(record.getPrecision(), convertedValues);
    }
}
