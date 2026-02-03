package com.example;

import com.example.benchmark.JacksonConvertValueBenchmark;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        JacksonConvertValueBenchmark benchmark = new JacksonConvertValueBenchmark();
        benchmark.runBenchmark();
    }
}