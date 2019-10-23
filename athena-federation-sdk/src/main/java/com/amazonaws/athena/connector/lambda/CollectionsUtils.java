package com.amazonaws.athena.connector.lambda;

import java.util.Collection;

public class CollectionsUtils
{
    private CollectionsUtils() {}

    public static boolean equals(Collection lhs, Collection rhs)
    {
        if (lhs == null && rhs == null) {
            return true;
        }

        if ((lhs == null && rhs != null) || (lhs != null && rhs == null)) {
            return false;
        }

        if (lhs.size() != rhs.size()) {
            return false;
        }

        return lhs.containsAll(rhs);
    }
}
