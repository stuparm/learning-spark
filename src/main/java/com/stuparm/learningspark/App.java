package com.stuparm.learningspark;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {

        // parse args into exec context if needed
        ExecContext ctx = new ExecContext();

        Executable example1 = new Example_01();
        example1.exec(ctx);
    }
}
