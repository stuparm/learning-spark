package com.stuparm.learningspark;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        // vm option: --add-exports java.base/sun.nio.ch=ALL-UNNAMED

        // parse args into exec context if needed
        ExecContext ctx = new ExecContext();

        Executable example = new Example06();
        example.exec(ctx);
    }
}
