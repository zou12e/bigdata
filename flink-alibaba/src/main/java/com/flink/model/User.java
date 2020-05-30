package com.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User implements Serializable {
    public String name;
    private int age;

    @Override
    public String toString() {
        return name + " : " + age;
    }


}
