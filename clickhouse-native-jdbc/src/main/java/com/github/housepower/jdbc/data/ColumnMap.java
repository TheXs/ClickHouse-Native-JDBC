/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.jdbc.data;

import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.jdbc.data.type.complex.DataTypeTuple;
import com.github.housepower.jdbc.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnMap extends AbstractColumn {

    private final List<Long> offsets;

    private final IColumn keyColumn;

    private final IColumn valueColumn;

    public ColumnMap(String name, DataTypeTuple type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        IDataType[] types = type.getNestedTypes();
        keyColumn = ColumnFactory.createColumn(null, types[0], null);
        valueColumn = ColumnFactory.createColumn(null, types[1], null);
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        ClickHouseStruct tuple = (ClickHouseStruct) object;
        Object[][] data = (Object[][]) tuple.getAttributes();
        offsets.add(offsets.isEmpty() ? data.length : offsets.get((offsets.size() - 1)) + data.length);
        for (Object[] entry : data) {
            keyColumn.write(entry[0]);
        }
        for (Object[] entry : data) {
            valueColumn.write(entry[1]);
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        flushOffsets(serializer);
        keyColumn.flushToSerializer(serializer, false);
        valueColumn.flushToSerializer(serializer, false);

        if (immediate) {
            buffer.writeTo(serializer);
        }
    }

    public void flushOffsets(BinarySerializer serializer) throws IOException, SQLException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        keyColumn.setColumnWriterBuffer(buffer);
        valueColumn.setColumnWriterBuffer(buffer);
    }

    @Override
    public void clear() {
        offsets.clear();
        keyColumn.clear();
        valueColumn.clear();
    }
}
