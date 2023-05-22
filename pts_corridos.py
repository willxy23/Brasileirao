import os
import mysql.connector
import pandas as pd


cnx = mysql.connector.connect(user='root', host='127.0.0.1', database='BRASILEIRAO')

#metodo cursor para manipular dados no banco 
cursor = cnx.cursor()


# lembrar de alterar no computador do prof
pasta_csv = r'C:\Users\Will\Desktop\Brasileirao csv'



# pegar os arquivos csv na pasta
for arquivo_csv in os.listdir(pasta_csv):
    if arquivo_csv.endswith('.csv'):
        
    # variavel que integra pasta e arquivos csv
        caminho_csv = os.path.join(pasta_csv, arquivo_csv)

        # vamos usar a função splitext para pegar o ano correpondente sem a extenção csv
        ano = os.path.splitext(arquivo_csv)[0]

        # Ler os arquivos usando a variavel caminho_csv
        data = pd.read_csv(caminho_csv)

        # Nome da tabela no banco de dados
        nome_tabela = f"tabela_brasileirao_{ano}"
        
        cursor.execute(f"""
                CREATE TABLE {nome_tabela} (
                    posicao INT,
                    time VARCHAR(255),
                    pts INT,
                    j INT,
                    v INT,
                    e INT,
                    d INT,
                    gp INT,
                    gc INT,
                    sg INT
                )
            """)

        # Iterar sobre as linhas do DataFrame e inserir os dados no MySQL    
        for _, row in data.iterrows():
            query = f"INSERT INTO {nome_tabela} (Posicao, time, pts, j, v, e, d, gp, gc, sg) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
            row['Posicao'], row['Time'], row['PTS'], row['J'], row['V'], row['E'], row['D'], row['GP'], row['GC'], row['SG']
            )
            
            cursor.execute(query, values)
            
cnx.commit()
cursor.close()
cnx.close()


