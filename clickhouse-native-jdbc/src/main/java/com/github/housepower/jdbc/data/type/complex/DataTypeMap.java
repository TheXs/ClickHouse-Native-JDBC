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

package com.github.housepower.jdbc.data.type.complex;

import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.jdbc.data.DataTypeFactory;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.misc.SQLLexer;
import com.github.housepower.jdbc.misc.Validate;
import com.github.housepower.jdbc.serde.BinaryDeserializer;
import com.github.housepower.jdbc.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class DataTypeMap implements IDataType {

    public static DataTypeCreator creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<IDataType> nestedDataTypes = new ArrayList<>();

        for (; ; ) {
            nestedDataTypes.add(DataTypeFactory.get(lexer, serverContext));
            char delimiter = lexer.character();
            Validate.isTrue(delimiter == ',' || delimiter == ')');
            if (delimiter == ')') {
                StringBuilder builder = new StringBuilder("Map(Tuple(");
                for (int i = 0; i < nestedDataTypes.size(); i++) {
                    if (i > 0)
                        builder.append(",");
                    builder.append(nestedDataTypes.get(i).name());
                }
                return new DataTypeMap(builder.append("))").toString(),
                        new DataTypeTuple("Tuple(", nestedDataTypes.toArray(new IDataType[0])),
                        DataTypeFactory.get("UInt64", serverContext));
            }
        }
    };

    private final String name;
    private final DataTypeTuple dataTypeTuple;
    private final IDataType[] nestedTypes;
    private final IDataType offsetIDataType;

    public DataTypeMap(String name, DataTypeTuple dataTypeTuple,IDataType offsetIDataType) {
        this.name = name;
        this.dataTypeTuple = dataTypeTuple;
        this.nestedTypes = dataTypeTuple.getNestedTypes();
        this.offsetIDataType = offsetIDataType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.STRUCT;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public Class javaType() {
        return Struct.class;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[dataTypeTuple.getNestedTypes().length];
        for (int i = 0; i < dataTypeTuple.getNestedTypes().length; i++) {
            attrs[i] = dataTypeTuple.getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return new ClickHouseStruct("Map", attrs);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {

    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        boolean readData = rows > 0;
        if (!readData) {
            return new Struct[0];
        }
        final int offset = ((BigInteger)offsetIDataType.deserializeBinary(deserializer)).intValue();
        Object[] keys = readValueList(true, rows, deserializer, offset);
        Object[] values = readValueList(false, rows, deserializer, offset);

        Struct[] rowsData = new Struct[rows];
        for (int row = 0; row < rows; row++) {
            Object[][] elemsData = new Object[offset][2];

            for (int elemIndex = 0; elemIndex < offset; elemIndex++) {
                elemsData[elemIndex] = new Object[]{keys[elemIndex], values[elemIndex]};
            }
            rowsData[row] = new ClickHouseStruct("Map", elemsData);
        }
        return rowsData;
    }

    private Object[] readValueList(boolean key, int rows, BinaryDeserializer deserializer, int offset)
            throws IOException, SQLException {
        Object[] rowsWithElems = new Object[offset];
        for (int index = 0; index < offset; index++) {
            rowsWithElems[index] = nestedTypes[key ? 0 : 1].deserializeBinaryBulk(rows, deserializer);
        }
        return rowsWithElems;
    }

    @Override
    public Object deserializeTextQuoted(SQLLexer lexer) throws SQLException {
        return null;
    }

    public DataTypeTuple getDataTypeTuple() {
        return dataTypeTuple;
    }
}
