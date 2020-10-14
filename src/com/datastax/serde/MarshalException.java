package com.datastax.serde;

public class MarshalException extends RuntimeException
{
    public MarshalException(String message)
    {
        super(message);
    }

    public MarshalException(String message, Throwable cause)
    {
        super(message, cause);
    }
}