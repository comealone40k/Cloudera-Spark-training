////////////// Reducing

// Textfile from HDFS
val rdd = sc.textFile("/user/komlosila/Spark/Orders/orders.csv")

// Removing header row
val header = rdd.first
val headlessrdd = rdd.filter(sale => { sale != header })

// Filter
val filteredrdd = headlessrdd.filter( row => {
val region = row.split(",")(12)
region == "West" || region == "East"
})

// Regexp to properly split comma separated text
val r = """,(?=([^\"]*\"[^\"]*\")*[^\"]*$)"""

// Creating a sales map
val salesMap = filteredrdd.map( sale => {
  sale.split(r)(17).toFloat
})

// Sample saleMap
salesMap.take(10).foreach(println)
val totalSales = salesMap.reduce((runningSales, sale) => runningSales + sale)

// Creating a profits map
val profitsMap = filteredrdd.map( profit => {
  profit.split(r)(20).toFloat
})
val totalProfits = profitsMap.reduce((runningProfits, sale) => runningProfits + sale)

// Calculating profit margin
val profitMargin = totalProfits / totalSales

// Average sales per customer
// Number of customers
val numCustomers = filteredrdd.map( sale => {
  sale.split(r)(5)
}).distinct.count

val avg_sales_per_cust = totalSales / numCustomers

// Average profit on a sale
val avg_proft_per_sale = totalProfits / filteredrdd.count