# -*- coding: utf-8 -*-

# ---Informacoes da aplicacao---
# Processo criado para obtencao de dados de municipios Ibge
# Onde obtemos de um Endpoint os dados de municipios por meio de um json
# Tranformado esse json e formato tabela e gravado na database_bi
# Sera tambem obtido os municipios do azuresqldb
# Depois é feito uma juncao dessas duas tabelas e gravado os dados que possuem relacao em uma tabela final da database_bi

# ---requisitos de sistema---
# python 3.0 ou superior
# modulos do python: requests, os, io, dotenv, datetime, pyodbc
# modulo de usuario: removeLogAntigo

import requests
import os, io
import dotenv
from datetime import datetime
import pyodbc as po
from removeLogAntigo import removeLogs

## Carrega os valores do .env que contem os dados sensíveis de conexao
dotenv.load_dotenv()

## Caminho raiz da aplicacao
PathRoot = os.path.dirname(os.path.abspath(__file__))

## funcao de formacao da connString DatabaseBi
def strConnectionDatabaseBi():
    
    #variaveis de conexao database_bi
    server   = os.getenv("SERVER_BI_SQL")
    port     = os.getenv("PORT_BI_SQL")
    database = os.getenv("DATABASE_BI_SQL")
    username = os.getenv("USERNAME_BI_SQL")
    password = os.getenv("PASSWORD_BI_SQL")

    strConnection = 'DRIVER={{ODBC Driver 17 for SQL Server}};\
        SERVER={v_server};\
        PORT={v_port};\
        DATABASE={v_database};\
        UID={v_username};\
        PWD={v_password}'.format(v_server = server, v_port = port, v_database = database, v_username = username, v_password = password)

    return strConnection


## funcao de formacao da connString AzureSql
def strConnectionAzureSql():
    
    #variaveis de conexao azuresql
    server   = os.getenv("SERVER_AZURESQL")
    port     = os.getenv("PORT_AZURESQL")
    database = os.getenv("DATABASE_AZURESQL")
    username = os.getenv("USERNAME_AZURESQL")
    password = os.getenv("PASSWORD_AZURESQL")

    strConnection = 'DRIVER={{ODBC Driver 17 for SQL Server}};\
        SERVER={v_server};\
        PORT={v_port};\
        DATABASE={v_database};\
        UID={v_username};\
        PWD={v_password}'.format(v_server = server, v_port = port, v_database = database, v_username = username, v_password = password)

    return strConnection


## Grava Log
def GravaLog(strValue, strAcao):

    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(PathRoot, 'log')
    pathLogFile = os.path.join(pathLog, 'logEndPointIbge_{0}.txt'.format(datahoraLog))

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## le os dados em json via requests
def obterJson():

    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = 'Obtendo dados do EndPoint: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    ## variaveis endpoint
    v_bearer_token=os.getenv("bearer_token")
    v_url_endpoint=os.getenv("url_endpoint")

    bearer_token=v_bearer_token
    headers = {"Authorization": f"Bearer {bearer_token}"}
    response = requests.get(v_url_endpoint, headers=headers)
    retorno = response.json()
    
    return retorno


## grava valores do json em formato csv
def gravaCsvfromJson(listEndPoint):

    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = 'Gravando dados em formato csv: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    ## Path CsvFile
    pathCsv = os.path.join(PathRoot, 'csv')
    pathCsvFile = os.path.join(pathCsv, 'FileCSVData.csv')

    ## cria a pasta de csv se não existir
    if not os.path.exists(pathCsv):
        os.makedirs(pathCsv)
    else:
        pass

    ## escreve o csv apagando conteúdo anterior
    with open(pathCsvFile, 'w', newline='', encoding='utf-8') as data_file:
        for i in range(len(listEndPoint)):
            
            ## valores obtidos da lista
            v_codigoIBGE = str(listEndPoint[i]['codIBGE'])
            v_lastVersao = str(listEndPoint[i]['lastVersao'])
            v_updateLayout = 1 if str(listEndPoint[i]['updateLayout']) == 'True' else 0
            csvLine = ('{0},{1},{2}\n').format(v_codigoIBGE, v_ultimaVersao, v_atualizacaoLayout)
            data_file.write(csvLine)


## gera comandos de inserts conforme valores da lista passada
def gravaDadosEnpointAux(listEndPoint):

    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = 'Gravando dados do endpoint em tabela sql: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    try:

        ## Connection string
        connString = str(strConnectionDatabaseBi())
        cnxn = po.connect(connString)
        
        ## query de busca
        cursor = cnxn.cursor()

        RowCount = 0
        msgLog = 'Limpando a tabela [dbo].[JsonEndpointIbge]'
        print(GravaLog(msgLog, 'a'))
        sqlcmdCreateTable = 'TRUNCATE TABLE [dbo].[JsonEndpointIbge];'
        cursor.execute(sqlcmdCreateTable)
        cnxn.commit()

        for i in range(len(listEndPoint)):
            
            ## valores obtidos da lista
            v_codigoIBGE = str(listEndPoint[i]['codigoIBGE'])
            v_lastVersao = str(listEndPoint[i]['lastVersao'])
            v_updateLayout = 1 if str(listEndPoint[i]['updateLayout']) == 'True' else 0

            sqlcmd = 'INSERT INTO [dbo].[JsonEndpointIbge] (codigoIBGE, lastVersao, updateLayout) VALUES (?, ?, ?);'
            paramSql = (v_codigoIBGE, v_lastVersao, v_updateLayout)
            cursor.execute(sqlcmd, paramSql)
            RowCount = RowCount + cursor.rowcount

    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela [dbo].[JsonEndpointIbge] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    else:
        cnxn.commit()
        
    finally:
        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()
        msgLog = 'Quantidade de Registros Inseridos na tabela [dbo].[JsonEndpointIbge]: {0}'.format(RowCount)
        print(GravaLog(msgLog, 'a'))
        obterDadosCodigosMunicipiosIbge()


## Obtem dados da tabela [dbo].[CodMunicipiosIbge] 
def obterDadosCodigosMunicipiosIbge():

    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = 'Obtendo dados da tabela [dbo].[MunicipiosIbge]: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    try:

        ## Connection string
        connString = str(strConnectionAzureSql())
        cnxn = po.connect(connString)
        
        ## query de busca
        cursor = cnxn.cursor()

        sqlcmd = '''
        SELECT
             [CodigoMunicipio]
            ,[Municipio]
            ,[UFSigla]
            ,[UFNome]
        FROM [dbo].[CodMunicipiosIbge]
        '''

        listSqlCodigosIbge = []
        cursor.execute(sqlcmd)
        listSqlCodigosIbge = list(cursor.fetchall())

    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Obtenção de dados na tabela [dbo].[CodMunicipiosIbge] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))
       
    finally:
        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()

        if not listSqlCodigosIbge:
            datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            msgLog = 'Não foi possível obter dados de Município IBGE\n'
            msgLog = '{0}\n***** Fim da aplicação: {1}\n'.format(msgLog, datahora)
            print(GravaLog(msgLog, 'a'))
            exit()
        else:
            qtdRegistros = len(listSqlCodigosIbge)
            msgLog = 'Fim Obtenção de dados na tabela [dbo].[CodMunicipiosIbge], {0} registros obtidos'.format(qtdRegistros)
            print(GravaLog(msgLog, 'a'))
            gravaDadosMunicipioAux(listSqlCodigosIbge)


## grava dados obtidos da tabela [dbo].[CodMunicipiosIbge] para a tabela Auxiliar na DatabaseBi
def gravaDadosMunicipioAux(listMunicipiosAux):

    try:
        ## Connection string
        connString = str(strConnectionDatabaseBi())
        cnxn = po.connect(connString)
        
        ## query de busca
        cursor = cnxn.cursor()

        RowCount = 0
        msgLog = 'Limpando a tabela [dbo].[MunicipiosIbgeAux]'
        print(GravaLog(msgLog, 'a'))
        sqlcmdCreateTable = 'TRUNCATE TABLE [dbo].[MunicipiosIbgeAux];'
        cursor.execute(sqlcmdCreateTable)
        cnxn.commit()

        for i in range(len(listMunicipiosAux)):
            
            ## valores obtidos da lista
            v_CodigoMunicipio = str(listMunicipiosAux[i][0])
            v_Municipio = str(listMunicipiosAux[i][1]) ##.replace("'", "''") ##tratamento de strings para aspas simples
            v_UFSigla = str(listMunicipiosAux[i][2])
            v_UFNome = str(listMunicipiosAux[i][3])

            sqlcmd = 'INSERT INTO [dbo].[MunicipiosIbgeAux] ([CodigoMunicipio], [Municipio], [UFSigla], [UFNome]) VALUES (?, ?, ?, ?);'
            paramSql = (v_CodigoMunicipio, v_Municipio, v_UFSigla, v_UFNome)
            cursor.execute(sqlcmd)
            RowCount = RowCount + cursor.rowcount

    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela [dbo].[MunicipiosIbgeAux] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    else:
        cnxn.commit()
        
    finally:
        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()
        msgLog = 'Quantidade de Registros Inseridos na tabela [dbo].[MunicipiosIbgeAux]: {0}'.format(RowCount)
        print(GravaLog(msgLog, 'a'))


## realiza o merge dos dados atualizando ou inserindo municipios novos na tabela final na DatabaseBi
def atualizaInsereMunicipiosIBGE():

    try:

        ## Connection string
        connString = str(strConnectionDatabaseBi())
        cnxn = po.connect(connString)
        
        ## query de busca
        cursor = cnxn.cursor()

        sqlcmd = '''
        ; WITH CteMunicipiosIBGE AS (
            SELECT
                M.Municipio,
                M.UFSigla,
                E.codigoIBGE,
                E.lastVersao,
                E.updateLayout
            FROM [dbo].[MunicipiosIbgeAux] M
            INNER JOIN [dbo].[JsonEndpointIbge] E ON E.codigoIBGE = M.CodigoMunicipio
        ) 
        MERGE 
            [dbo].[MunicipiosIBGE] AS Destino 
        USING
            CteMunicipiosIBGE AS Origem ON (Origem.codigoIBGE = Destino.CodigoIBGE)

        -- Registro existe nas 2 tabelas
        WHEN MATCHED THEN
            UPDATE SET 
                Destino.Municipio = Origem.Municipio,
                Destino.UFSigla = Origem.UFSigla,
                Destino.codigoIBGE = Origem.codigoIBGE,
                Destino.ultimaVersao = Origem.ultimaVersao,
                Destino.atualizacaoLayout = Origem.atualizacaoLayout
                
        -- Registro não existe no destino. Vamos inserir.
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            VALUES(Origem.Municipio, Origem.UFSigla, Origem.codigoIBGE, Origem.ultimaVersao, Origem.atualizacaoLayout, 1);
        '''

        cursor.execute(sqlcmd)

    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela [dbo].[MunicipiosIBGE] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    else:
        cnxn.commit()
        
    finally:
        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()
        msgLog = 'Fim Insert tabela [dbo].[MunicipiosIBGE]'
        print(GravaLog(msgLog, 'a'))


## funcao inicial criada para iniciar as chamadas das demais funcoes
def main():
    ## log do início da aplicacao
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = '\n***** Início da aplicação: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    listEndPoint = list(obterJson())

    if not listEndPoint:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgLog = 'Não existem dados para processar'
        msgLog = '{0}\n***** Final da aplicação: {1}\n'.format(msgLog, datahora)
        print(GravaLog(msgLog, 'a'))
        exit()
    else: 
        #gravaCsvfromJson(listEndPoint)
        gravaDadosEnpointAux(listEndPoint)
        atualizaInsereMunicipiosIBGE()

    ## remocao dos logs antigos acima de xx dias
    diasRemover = 10
    dirLog = os.path.join(PathRoot, 'log')
    msgLog = removeLogs(diasRemover, dirLog)
    print(GravaLog(msgLog, 'a'))

    ## log do final da aplicacao
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgLog = '***** Final da aplicação: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da função inicial
    main()
