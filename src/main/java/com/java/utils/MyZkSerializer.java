package com.java.utils;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.spark_project.guava.base.Charsets;

public class MyZkSerializer implements ZkSerializer
{
    public Object deserialize(byte[] bytes) throws ZkMarshallingError
    {
        return new String(bytes, Charsets.UTF_8);
    }

    public byte[] serialize(Object obj) throws ZkMarshallingError
    {
        return String.valueOf(obj).getBytes(Charsets.UTF_8);
    }
}