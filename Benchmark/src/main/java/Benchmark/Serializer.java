package Benchmark;

import Benchmark.Generator.GeneratedData.GeneratedFloor;

import java.io.*;
import java.nio.file.Paths;
import java.util.Random;

public class Serializer {
    private static final String FLOOR_FILE_NAME = "floors.ser";
    private static final String RNG_FILE_NAME = "random.ser";

    public static void serializeFloor(GeneratedFloor[] generatedFloors, String writeFolder) throws IOException {
        write(Paths.get(writeFolder, FLOOR_FILE_NAME).toString(), generatedFloors);
    }

    public static void serializeRandom(Random rng, String writeFolder) throws IOException {
        write(Paths.get(writeFolder, RNG_FILE_NAME).toString(), rng);
    }

    public static GeneratedFloor[] deserializeFloor(String readFolder) throws IOException, ClassNotFoundException{
        return (GeneratedFloor[]) read(Paths.get(readFolder, FLOOR_FILE_NAME).toString());
    }

    public static Random deserializeRandom(String readFolder) throws IOException, ClassNotFoundException{
        return (Random) read(Paths.get(readFolder, RNG_FILE_NAME).toString());
    }

    private static void write(String path, Object object) throws IOException{
        try(FileOutputStream outFile = new FileOutputStream(path);
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(object);
        }
    }

    public static Object read(String path) throws IOException, ClassNotFoundException{
        try(FileInputStream inFile = new FileInputStream(path);
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            return inStream.readObject();
        }
    }
}
