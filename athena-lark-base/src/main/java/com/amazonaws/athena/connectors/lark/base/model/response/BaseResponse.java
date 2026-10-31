/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.model.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Base response class for Lark API responses.
 * If class extends this class, would be better using builder pattern.
 * If class does not extend this class, prefer using record class.
 *
 * @param <T> The type of the data in the response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = BaseResponse.Builder.class)
public class BaseResponse<T>
{
    private final int code;
    private final String msg;
    private final T data;

    protected BaseResponse(Builder<T> builder)
    {
        this.code = builder.code;
        this.msg = builder.msg;
        this.data = builder.data;
    }

    @JsonProperty("code")
    public int getCode()
    {
        return code;
    }

    @JsonProperty("msg")
    public String getMsg()
    {
        return msg;
    }

    @JsonProperty("data")
    public T getData()
    {
        return data;
    }

    public static <T> Builder<T> builder()
    {
        return new Builder<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder<T>
    {
        protected int code;
        protected String msg;
        protected T data;

        public Builder()
        {
        }

        @JsonProperty("code")
        public Builder<T> code(int code)
        {
            this.code = code;
            return this;
        }

        @JsonProperty("msg")
        public Builder<T> msg(String msg)
        {
            this.msg = msg;
            return this;
        }

        @JsonProperty("data")
        public Builder<T> data(T data)
        {
            this.data = data;
            return this;
        }

        public BaseResponse<T> build()
        {
            return new BaseResponse<>(this);
        }
    }
}
