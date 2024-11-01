from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, to_date, when, mean, year, date_format, count, first
from pyspark.sql.types import IntegerType, StringType
import matplotlib.pyplot as plt
import emoji
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import yfinance as yf

# Avvia una sessione Spark
spark = SparkSession.builder.appName("BitcoinTweetsAnalysis").getOrCreate()

# Leggi il file CSV in un DataFrame PySpark
df = spark.read.csv('bitcoin_tweets.csv', header=True, inferSchema=True)

# Rimuovi colonne inutili
df = df.drop('user', 'fullname', 'url')

# Converti 'timestamp' in formato data e ordina
df = df.withColumn('timestamp', to_date(col('timestamp')))
df = df.orderBy(col('timestamp').asc())

# Definisci una UDF per preprocessare il testo
def preprocess_text(text):
    if text is None:
        return ''
    # Rimuovi URL
    text = re.sub(r'http\S+', '', text)
    # Rimuovi menzioni
    text = re.sub(r'@\w+', '', text)
    # Converti le emoji in testo
    text = emoji.demojize(text)
    # Rimuovi caratteri speciali
    text = re.sub(r'[^A-Za-z0-9\s:]', '', text)
    return text.strip()

preprocess_text_udf = udf(preprocess_text, StringType())

# Applica la UDF alla colonna 'text'
df = df.withColumn('text', preprocess_text_udf(col('text')))

# Definisci una UDF per l'analisi del sentiment
def analyze_sentiment_vader(text):
    analyzer = SentimentIntensityAnalyzer()
    if text is None:
        text = ''
    score = analyzer.polarity_scores(text)
    if score['compound'] >= 0:
        return 1
    else:
        return 0

analyze_sentiment_udf = udf(analyze_sentiment_vader, IntegerType())

# Applica la UDF per ottenere il 'sentiment'
df = df.withColumn('sentiment', analyze_sentiment_udf(col('text')))

# Estrai la data dal timestamp
df = df.withColumn('date', date_format(col('timestamp'), 'yyyy-MM-dd'))

# Raggruppa per data e calcola la media del sentiment
sentiment_per_day = df.groupBy('date').agg(mean('sentiment').alias('mean_sentiment'))

# Converte il risultato in Pandas per la visualizzazione
sentiment_per_day_pd = sentiment_per_day.orderBy('date').toPandas()

# Crea un grafico
plt.figure(figsize=(10, 6))
plt.plot(sentiment_per_day_pd['date'], sentiment_per_day_pd['mean_sentiment'], linestyle='-', color='b')

# Aggiungi titolo e etichette agli assi
plt.title('Variazione del Sentiment nel Tempo')
plt.xlabel('Data')
plt.ylabel('Media del Sentiment (1 = Positivo, 0 = Negativo)')

# Ruota le etichette delle date per una migliore leggibilità
plt.xticks(rotation=45)

# Mostra il grafico
plt.tight_layout()
plt.show()

# Raggruppa per sentiment e calcola la media dei likes
grouped_likes_mean = df.groupBy('sentiment').agg(mean('likes').alias('mean_likes'))

# Converte il risultato in Pandas per la visualizzazione
grouped_likes_mean_pd = grouped_likes_mean.orderBy('sentiment').toPandas()

# Crea un grafico a barre per la media dei likes
fig, ax = plt.subplots(figsize=(8, 6))
grouped_likes_mean_pd.plot(kind='bar', x='sentiment', y='mean_likes', color=['red', 'green'], ax=ax, legend=False)

# Aggiungi etichette sopra le barre
for i, value in enumerate(grouped_likes_mean_pd['mean_likes']):
    ax.text(i, value, f'{value:.0f}', ha='center', va='bottom')

# Aggiungi etichette e titolo
plt.title('Media dei likes per sentiment')
plt.xlabel('Sentiment (0 = Negativo, 1 = Positivo)')
plt.ylabel('Media dei Likes')

# Visualizza il grafico
plt.show()

# Raggruppa per sentiment e calcola la media dei replies
grouped_replies_mean = df.groupBy('sentiment').agg(mean('replies').alias('mean_replies'))
grouped_replies_mean_pd = grouped_replies_mean.orderBy('sentiment').toPandas()

# Crea un grafico a barre per la media dei replies
fig, ax = plt.subplots(figsize=(8, 6))
grouped_replies_mean_pd.plot(kind='bar', x='sentiment', y='mean_replies', color=['red', 'green'], ax=ax, legend=False)

# Aggiungi etichette sopra le barre
for i, value in enumerate(grouped_replies_mean_pd['mean_replies']):
    ax.text(i, value, f'{value:.2f}', ha='center', va='bottom')

# Aggiungi etichette e titolo
plt.title('Media dei replies per sentiment')
plt.xlabel('Sentiment (0 = Negativo, 1 = Positivo)')
plt.ylabel('Media dei Replies')

# Visualizza il grafico
plt.show()

# Raggruppa per sentiment e calcola la media dei retweets
grouped_retweets_mean = df.groupBy('sentiment').agg(mean('retweets').alias('mean_retweets'))
grouped_retweets_mean_pd = grouped_retweets_mean.orderBy('sentiment').toPandas()

# Crea un grafico a barre per la media dei retweets
fig, ax = plt.subplots(figsize=(8, 6))
grouped_retweets_mean_pd.plot(kind='bar', x='sentiment', y='mean_retweets', color=['red', 'green'], ax=ax, legend=False)

# Aggiungi etichette sopra le barre
for i, value in enumerate(grouped_retweets_mean_pd['mean_retweets']):
    ax.text(i, value, f'{value:.2f}', ha='center', va='bottom')

# Aggiungi etichette e titolo
plt.title('Media dei retweets per sentiment')
plt.xlabel('Sentiment (0 = Negativo, 1 = Positivo)')
plt.ylabel('Media dei Retweets')

# Visualizza il grafico
plt.show()

# Crea una colonna 'year' per estrarre l'anno dalla data
df = df.withColumn('year', year(col('timestamp')))

# Raggruppa per anno e sentiment e conta i valori
grouped_df = df.groupBy('year', 'sentiment').agg(count('*').alias('count'))

# Pivota il DataFrame per avere i sentiment come colonne
grouped_pivot_df = grouped_df.groupBy('year').pivot('sentiment', [0, 1]).agg(first('count'))

# Sostituisci i valori nulli con 0
grouped_pivot_df = grouped_pivot_df.na.fill(0)

# Ordina per anno
grouped_pivot_df = grouped_pivot_df.orderBy('year')

# Converte in Pandas per la visualizzazione
grouped_pivot_pd = grouped_pivot_df.toPandas()

# Crea l'istogramma impilato
fig, ax = plt.subplots(figsize=(10,6))
grouped_pivot_pd.plot(x='year', kind='bar', stacked=True, color=['red', 'green'], ax=ax)

# Aggiungi etichette sopra le barre
for container in ax.containers:
    for bar in container:
        height = bar.get_height()
        if height > 100:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_y() + height + 0.5,
                f'{int(height)}',
                ha='center', va='bottom'
            )

# Aggiungi etichette e titolo
plt.title('Sentiment positivi e negativi per anno')
plt.xlabel('Anno')
plt.ylabel('Numero di sentiment')
plt.legend(['Negativi', 'Positivi'])

# Visualizza il grafico
plt.show()

# Scarica i dati storici del Bitcoin
bitcoin_data = yf.download('BTC-USD', start='2009-01-01', end='2020-01-01')

# Crea un grafico dell'andamento del Bitcoin
plt.figure(figsize=(10,6))
plt.plot(bitcoin_data['Close'], label='Bitcoin Price (USD)')
plt.title('Andamento del Bitcoin')
plt.xlabel('Data')
plt.ylabel('Prezzo in USD')
plt.legend()
plt.grid(True)

# Visualizza il grafico
plt.show()

# Ferma la sessione Spark
spark.stop()
