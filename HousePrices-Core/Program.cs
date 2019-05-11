using System;
using Microsoft.Spark.Sql;

namespace HousePrices
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var dataFrame = spark.Read().Option("inferSchema", true).Csv(args[0]);

            dataFrame = dataFrame.ToDF("file_guid", "price", "date_str", "post_code", "property_type", "old_new", "duration", "paon", "saon", "street", "locality", "town", "district", "county", "ppd_Category_type", "record_type");

            dataFrame = dataFrame.Where("district = 'KENSINGTON AND CHELSEA'");

            Console.WriteLine($"There are {dataFrame.Count()} properties in Holland Park");

            dataFrame = dataFrame.WithColumn("date", dataFrame.Col("date_str").Cast("date"));

            dataFrame.CreateTempView("ppd");

            spark.Sql("select year(date), avg(price), count(*) from ppd group by year(date)").OrderBy(Functions.Year(dataFrame.Col("date")).Desc()).Show(100);
        }
    }
}