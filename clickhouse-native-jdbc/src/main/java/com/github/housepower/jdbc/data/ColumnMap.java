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

    private final List<Object[][]> data;

    private final DataTypeTuple dataTypeTuple;

    public ColumnMap(String name,DataTypeTuple type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        data = new ArrayList<>();
        dataTypeTuple = type;
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        ClickHouseStruct tuple = (ClickHouseStruct) object;
        Object[][] dataArr = (Object[][]) tuple.getAttributes();
        offsets.add(offsets.isEmpty() ? dataArr.length : offsets.get((offsets.size() - 1)) + dataArr.length);
        data.add(dataArr);
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        // 偏移
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
        // Tuple
        for (Object[][] dataArr : data) {
            ClickHouseStruct [] dataBulk = new ClickHouseStruct[dataArr.length];
            int index = 0;
            for (Object[] data : dataArr) {
                dataBulk [index++] = new ClickHouseStruct("Tuple", data);
            }
            dataTypeTuple.serializeBinaryBulk(dataBulk, serializer);
        }
        if (immediate) {
            buffer.writeTo(serializer);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
    }

    @Override
    public void clear() {
        offsets.clear();
    }
}
