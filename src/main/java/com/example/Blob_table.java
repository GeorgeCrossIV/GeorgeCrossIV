package com.example;
import java.util.UUID;
import java.util.List;
import java.nio.ByteBuffer;

//@Entity
public class Blob_table {
    //@PartitionKey
    private String key;
    private UUID column1;
    private ByteBuffer value;

    public Blob_table();

    public Blob_table(String key, UUID column1, ByteBuffer value) {
        this.key = key;
        this.column1 = column1;
        this.value = value;
    }

    public String getKey() {return this.key;}
    public void setKey(String key) {this.key = key;}
    public UUID getColumn1() {return this.column1;}
    public void setColumn1(UUID column1) {this.column1 = column1;}
    public ByteBuffer getValue() {return this.value;}
    public void setValue(ByteBuffer value) {this.value = value;}

    public void print() {
        System.out.println(
            "key:" + this.key
             + "; column1: " + this.column1 
             + "; value: " + this.value
        );    
    }
}


