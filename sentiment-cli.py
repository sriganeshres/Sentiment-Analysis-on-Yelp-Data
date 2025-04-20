import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import mlflow.spark
import sys

def predict_text(text, lr_model, rf_model, spark):
    # Create DataFrame from input
    df = spark.createDataFrame([(text,)], ["text"])

    # Get predictions
    lr_pred = lr_model.transform(df).select("text", col("prediction").alias("lr_pred"))
    rf_pred = rf_model.transform(df).select("text", col("prediction").alias("rf_pred"))

    # Join predictions
    joined = lr_pred.join(rf_pred, "text")

    # Ensemble vote
    final_pred = joined.withColumn(
        "prediction",
        expr("int((lr_pred + rf_pred) / 2)")
    )

    final_pred.select("text", "lr_pred", "rf_pred", "prediction").show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ensemble Sentiment Classifier")
    parser.add_argument("--file", type=str, help="Optional input file with one sentence per line")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SentimentCLI").getOrCreate()

    print("Loading models from MLflow...")
    lr_model = mlflow.spark.load_model("runs:/74007486a54747f2aa7980e8ca59ce32/lr_model")
    rf_model = mlflow.spark.load_model("runs:/74007486a54747f2aa7980e8ca59ce32/rf_model")

    try:
        if args.file:
            with open(args.file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        print(f"\nInput: {line}")
                        predict_text(line, lr_model, rf_model, spark)
        else:
            print("Enter text to classify (type 'exit' to quit):")
            while True:
                line = input(">> ").strip()
                if line.lower() == "exit":
                    break
                if line:
                    predict_text(line, lr_model, rf_model, spark)
    except KeyboardInterrupt:
        print("\nExiting...")

    spark.stop()
