import org.apache.spark.sql.{DataFrame, functions => F}

object Transform {

  /**
   * Cette fonction permet de nettoyer les données brutes.
   * @param df : Dataframe
   * @return Dataframe
   */
  def cleanData(df: DataFrame): DataFrame = {
    /*
    0. Traiter toutes les colonnes en date timestamp vers YYYY/MM/DD HH:MM:SS.SSS.
     */
    val firstDF = df.withColumn("timestamp", F.date_format(F.col("timestamp"), "yyyy/MM/dd HH:mm:ss.SSS"))

    /*
    1. Extraire les revenus d'achat pour chaque événement
      - Ajouter une nouvelle colonne nommée revenue en faisant l'extration de ecommerce.purchase_revenue_in_usd
     */
    val revenueDF = firstDF.withColumn("revenue", F.col("ecommerce.purchase_revenue_in_usd").cast("double"))

    /*
    2. Filtrer les événements dont le revenu n'est pas null
    */
    val purchasesDF = revenueDF.filter(F.col("revenue").isNotNull)

    /*
    3. Quels sont les types d'événements qui génèrent des revenus ?
      Trouvez des valeurs event_name uniques dans purchasesDF.
      Combien y a-t-il de types d'événements ?
     */
    val distinctDF = purchasesDF.dropDuplicates("event_name")

    /*
     4. Supprimer la/les colonne(s) inutile(s)
      - Supprimez event_name de purchasesDF.
      */
    val cleanDF = distinctDF.drop("event_name")

    cleanDF
  }

  /**
   * Cette fonction permet de récupérer le revenu cumulé par source de traffic, par état et par ville
   * @param df : Dataframe
   * @return Dataframe
   */
  def computeTrafficRevenue(df: DataFrame): DataFrame = {
    /*
    5. Revenus cumulés par source de trafic par état et par ville
      - Obtenir la somme de revenue comme total_rev
      - Obtenir la moyenne de revenue comme avg_rev
     */
    val trafficDF = df.groupBy("traffic_source", "state", "city")
      .agg(
        F.sum("revenue").alias("total_rev"),
        F.avg("revenue").alias("avg_rev")
      )

    /*
    6. Récupérer les cinq principales sources de trafic par revenu total
     */
    val topTrafficDF = trafficDF.orderBy(F.desc("total_rev")).limit(5)

    /*
    7. Limitez les colonnes de revenus à deux décimales pointées
      Modifier les colonnes avg_rev et total_rev pour les convertir en des nombres avec deux décimales pointées
     */
    val finalDF = topTrafficDF
      .withColumn("total_rev", F.round(F.col("total_rev"), 2))
      .withColumn("avg_rev", F.round(F.col("avg_rev"), 2))

    finalDF
  }
}




