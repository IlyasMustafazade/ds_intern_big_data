import pyspark as ps
import pyspark.sql as psql
import pyspark.pandas as spd
import pyspark.sql.functions as sql_f


def main():
    tester = SparkSQLTester()
    tester.test_SparkSession()
    tester.test_load()

    tester.ss.stop()
    tester.outf.close()

    
class SparkSQLTester:
    def __init__(self):
        outf_name = "test_pyspark_sql"
        self.outf = open(f"{outf_name}.txt", "w")
        self.ss = None
        self.df = None

    def test_SparkSession(self):
        print(f"test_SparkSession output -->\n", file=self.outf)
        app_name = "Exploring pyspark.sql"
        master = "local"
        ss = psql.SparkSession.builder.appName(app_name).master(master).getOrCreate()
        self.ss = ss
        print(f"Type of ss: {type(self.ss)}\n", file=self.outf)
    
    def test_load(self):
        print(f"test_DataFrame output -->\n", file=self.outf)
        inf_path = "C:\\Users\\ilyas\\source\\repos\\ds_intern\\big_data\\resource\\w4\\Baby_Names__Beginning_2007.csv"
        df = self.ss.read.load(inf_path, format="csv", sep=",", inferSchema="true", header="true")
        self.df = df
        print(f"Dataframe:\n{self.df}\n", file=self.outf)
        print(f"Dataframe type:\n{type(self.df)}\n", file=self.outf)
    
    def test_toPandas(self):
        print(f"test_toPandas output -->\n", file=self.outf)
        pd_df = self.df.toPandas()
        print(f"As pandas Dataframe:\n{pd_df}\n", file=self.outf)
    
    def test_printSchema(self):
        print(f"test_printSchema output -->\n", file=self.outf)
        self.df.printSchema()
    
    def test_show(self):
        print(f"test_show output -->\n", file=self.outf)
        self.df.show(truncate=5)
    
    def test_count(self):
        print(f"test_count output -->\n", file=self.outf)
        count = self.df.count()
        print(f"Number of rows: {count}\n", file=self.outf)
    
    def test_columns(self):
        print(f"test_columns output -->\n", file=self.outf)
        print(f"Columns:\n{self.df.columns}\n", file=self.outf)
    
    def test_describe(self):
        print(f"test_describe output -->\n", file=self.outf)
        print(f"Description:\n{self.df.describe().toPandas()}\n", file=self.outf)
    
    def test_select(self):
        print(f"test_select output -->\n", file=self.outf)
        selected_df = self.df.select("First Name", "County")
        selected_df_pd = selected_df.toPandas()
        print(f"After selection:\n{selected_df_pd}\n", file=self.outf)
    
    def test_column(self):
        print(f"test_select output -->\n", file=self.outf)
        filtered_df = self.df.filter(sql_f.col("First Name") == "ZOE")
        filtered_df_pd = filtered_df.toPandas()
        print(f"After filter:\n{filtered_df_pd}\n", file=self.outf)
    
    def test_agg(self):
        print(f"test_agg output -->\n", file=self.outf)
        grouped_df = self.df.groupby("County")
        agg_df = grouped_df.agg({"Count": "mean"})
        agg_df_pd = agg_df.toPandas()
        print(f"After aggregation:\n{agg_df_pd}\n", file=self.outf)
    
    def test_withColumn(self):
        print(f"test_withColumn output -->\n", file=self.outf)
        new_col_name = "Gender"
        extended_df = self.df.withColumn(new_col_name,
            sql_f.when(sql_f.col("Sex") == "F", "Female").otherwise("Male"))
        extended_df_pd = extended_df.toPandas()    
        print(f"After extension:\n{extended_df_pd}\n", file=self.outf)
    
    def test_orderBy(self):
        print(f"test_orderBy output -->\n", file=self.outf)
        ordered_df = self.df.orderBy("Count")
        ordered_df_pd = ordered_df.toPandas()
        print(f"After ordering:\n{ordered_df_pd}\n", file=self.outf)
    
    def test_sql(self):
        print(f"test_sql output -->\n", file=self.outf)
        self.df.createOrReplaceTempView("df")
        filtered_df = self.ss.sql("select * from df where Count < 6")
        filtered_df = filtered_df.orderBy("Count", ascending=False)
        filtered_df_pd = filtered_df.toPandas()
        print(f"After filtering:\n{filtered_df_pd}\n", file=self.outf)

    def test_udf(self):
        print(f"test_udf output -->\n", file=self.outf)
        sc = self.ss.sparkContext
        self.ss.udf.register("udf_cube", lambda x: x * x * x)
        cubed_df = self.ss.sql("select udf_cube(2)")
        cubed_df = cubed_df.toPandas()
        print(f"After cube:\n{cubed_df}\n", file=self.outf)
    
    def test_join(self):
        print(f"test_join output -->\n", file=self.outf)
        lst1 = [('Pirate', 1), ('Monkey', 2), ('Ninja', 3), ('Spaghetti', 4)]
        lst2 = [('Rutabaga', 1), ('Pirate', 2), ('Ninja', 3), ('Darth Vader', 4)]
        df1 = self.ss.createDataFrame(lst1, ["name", "id"])
        df2 = self.ss.createDataFrame(lst2, ["name", "id"])
        i_join = df1.join(df2, on="name", how="inner")
        fo_join = df1.join(df2, on="name", how="fullouter")
        lo_join = df1.join(df2, on="name", how="left")
        ro_join = df1.join(df2, on="name", how="right")
        i_df = i_join.toPandas()
        fo_df = fo_join.toPandas()
        lo_df = lo_join.toPandas()
        ro_df = ro_join.toPandas()
        print(f"Inner join:\n{i_df}\n", file=self.outf)
        print(f"Full outer join:\n{fo_df}\n", file=self.outf)
        print(f"Left outer join:\n{lo_df}\n", file=self.outf)
        print(f"Right outer join:\n{ro_df}\n", file=self.outf)


if __name__ == "__main__": main()
