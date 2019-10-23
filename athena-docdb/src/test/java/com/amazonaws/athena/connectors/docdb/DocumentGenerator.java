package com.amazonaws.athena.connectors.docdb;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class DocumentGenerator
{
    private DocumentGenerator() {}

    /**
     * This should be replaced with something that actually reads useful data.
     */
    public static Document makeRandomRow(List<Field> fields, int seed)
    {
        Document result = new Document();

        for (Field next : fields) {
            boolean negative = seed % 2 == 1;
            Types.MinorType minorType = Types.getMinorTypeForArrowType(next.getType());
            switch (minorType) {
                case INT:
                    int iVal = seed * (negative ? -1 : 1);
                    result.put(next.getName(), iVal);
                    break;
                case TINYINT:
                case SMALLINT:
                    int stVal = (seed % 4) * (negative ? -1 : 1);
                    result.put(next.getName(), stVal);
                    break;
                case UINT1:
                case UINT2:
                case UINT4:
                case UINT8:
                    int uiVal = seed % 4;
                    result.put(next.getName(), uiVal);
                    break;
                case FLOAT4:
                    float fVal = seed * 1.1f * (negative ? -1 : 1);
                    result.put(next.getName(), fVal);
                    break;
                case FLOAT8:
                case DECIMAL:
                    double d8Val = seed * 1.1D * (negative ? -1 : 1);
                    result.put(next.getName(), d8Val);
                    break;
                case BIT:
                    boolean bVal = seed % 2 == 0;
                    result.put(next.getName(), bVal);
                    break;
                case BIGINT:
                    long lVal = seed * 1L * (negative ? -1 : 1);
                    result.put(next.getName(), lVal);
                    break;
                case VARCHAR:
                    String vVal = "VarChar" + seed;
                    result.put(next.getName(), vVal);
                    break;
                case VARBINARY:
                    byte[] binaryVal = ("VarChar" + seed).getBytes();
                    result.put(next.getName(), binaryVal);
                    break;
                case STRUCT:
                    result.put(next.getName(), makeRandomRow(next.getChildren(), seed));
                    break;
                case LIST:
                    //TODO: pretty dirty way of generating lists should refactor this to support better generation
                    Types.MinorType listType = Types.getMinorTypeForArrowType(next.getChildren().get(0).getType());
                    switch (listType) {
                        case VARCHAR:
                            List<String> listVarChar = new ArrayList<>();
                            listVarChar.add("VarChar" + seed);
                            listVarChar.add("VarChar" + seed + 1);
                            result.put(next.getName(), listVarChar);
                            break;
                        case INT:
                            List<Integer> listIVal = new ArrayList<>();
                            listIVal.add(seed * (negative ? -1 : 1));
                            listIVal.add(seed * (negative ? -1 : 1) + 1);
                            result.put(next.getName(), listIVal);
                            break;
                        default:
                            throw new RuntimeException(minorType + " is not supported in list");
                    }
                    break;
                default:
                    throw new RuntimeException(minorType + " is not supported");
            }
        }

        return result;
    }
}
