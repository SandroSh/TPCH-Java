import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO Import the stuff you need

public class Database {
    private static Path baseDataDirectory = Paths.get("data");
    private long sum;

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    public static Stream<Customer> processInputFileCustomer() {
        // TODO
        try {
            ArrayList<Customer> customerList = new ArrayList<>();

            String[] newArray;
            String line;
            BufferedReader reader = new BufferedReader(new FileReader(baseDataDirectory + "/customer.tbl"));
            while ((line = reader.readLine()) != null) {
                newArray = line.split("[|]");
                customerList.add(
                        new Customer(
                                Integer.parseInt(newArray[0]),
                                newArray[2].toCharArray(),
                                Integer.parseInt(newArray[3]),
                                newArray[4].toCharArray(),
                                Float.parseFloat(newArray[5]),
                                newArray[6],
                                newArray[7].toCharArray()));
            }

            return customerList.stream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    public static Stream<LineItem> processInputFileLineItem() {
        // TODO
        try {
            ArrayList<LineItem> lineItemList = new ArrayList<>();

            BufferedReader reader = new BufferedReader(new FileReader(baseDataDirectory + "/lineitem.tbl"));
            String[] array;
            String line;
            while ((line = reader.readLine()) != null) {
                array = line.split("[|]");
                lineItemList.add(
                        new LineItem(
                                Integer.parseInt(array[0]),
                                Integer.parseInt(array[1]),
                                Integer.parseInt(array[2]),
                                Integer.parseInt(array[3]),
                                Integer.parseInt(array[4]) * 100,
                                Float.parseFloat(array[5]),
                                Float.parseFloat(array[6]),
                                Float.parseFloat(array[7]),
                                array[8].charAt(0),
                                array[9].charAt(0),
                                LocalDate.of(
                                        Integer.parseInt(array[10].split("-")[0]),
                                        Integer.parseInt(array[10].split("-")[1]),
                                        Integer.parseInt(array[10].split("-")[2])),
                                LocalDate.of(
                                        Integer.parseInt(array[11].split("-")[0]),
                                        Integer.parseInt(array[11].split("-")[1]),
                                        Integer.parseInt(array[11].split("-")[2])),
                                LocalDate.of(
                                        Integer.parseInt(array[12].split("-")[0]),
                                        Integer.parseInt(array[12].split("-")[1]),
                                        Integer.parseInt(array[12].split("-")[2])),
                                array[13].toCharArray(),
                                array[14].toCharArray(),
                                array[15].toCharArray()));
            }

            return lineItemList.stream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

        // For quantity of LineItems please use Integer.parseInt(str) * 100.
    }

    public static Stream<Order> processInputFileOrders() {
        // TODO
        try {
            ArrayList<Order> ordersList = new ArrayList<>();

            BufferedReader reader = new BufferedReader(new FileReader(baseDataDirectory + "/orders.tbl"));
            String[] array;
            String line;
            while ((line = reader.readLine()) != null) {
                array = line.split("[|]");
                ordersList.add(
                        new Order(
                                Integer.parseInt(array[0]),
                                Integer.parseInt(array[1]),
                                array[2].charAt(0),
                                Float.parseFloat(array[3]),
                                LocalDate.of(
                                        Integer.parseInt(array[4].split("-")[0]),
                                        Integer.parseInt(array[4].split("-")[1]),
                                        Integer.parseInt(array[4].split("-")[2])),
                                array[5].toCharArray(),
                                array[6].toCharArray(),
                                Integer.parseInt(array[7]),
                                array[8].toCharArray()));
            }

            return ordersList.stream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        Map<Integer, String> mappedCustomerData = new HashMap<>();
        Map<Integer, String> mappedOrderData = new HashMap<>();

        processInputFileCustomer().forEach(item -> {

            mappedCustomerData.put(item.custKey, item.mktsegment);

        });

        processInputFileOrders().forEach(item -> {

            mappedOrderData.put(item.orderKey, mappedCustomerData.get(item.custKey));

        });

        Map<String, List<LineItem>> mappedMKTsegementData = processInputFileLineItem()
                .collect(Collectors.groupingBy(s -> mappedOrderData.get(s.orderKey)));

        mappedMKTsegementData.get(marketsegment).forEach(item -> {
            if (item != null) {
                sum += item.quantity;
            }

        });
        long quantity = mappedMKTsegementData.get(marketsegment).stream().count();
        return sum / quantity;
    }

    public Database() {
        // TODO

    }

    public static void main(String[] args) {
        // TODO Test your implementation

        Database data1 = new Database();

        System.out.println(data1.getAverageQuantityPerMarketSegment("AUTOMOBILE"));

    }
}
