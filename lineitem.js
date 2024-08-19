const fs = require("fs");
const oracledb = require("oracledb");
require('dotenv').config();
const BD_USER = process.env.BD_USER;
const BD_PASSWORD = process.env.BD_PASSWORD;
const BD_CONNECT = process.env.BD_CONNECT;

async function insertData() {
  let connection;

  try {
    // Conectando ao banco de dados Oracle
    connection = await oracledb.getConnection({
      user: BD_USER,
      password: BD_PASSWORD,
      connectString: BD_CONNECT,
    });

    console.log("Conectado ao Oracle Database");

    // Lendo o arquivo
    const data = fs.readFileSync(
      "D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\files\\lineitem.tbl",
      "utf8"
    );
    const lines = data.split("\n");

    // Ordenar as linhas por algum critério se necessário (aqui estamos apenas processando na ordem original)
    const sortedLines = lines.sort();

    // Iterando sobre cada linha do arquivo
    for (let line of sortedLines) {
      // Limpar espaços em branco e caracteres de nova linha
      line = line.trim().replace(/\r/g, ""); // Remove caracteres \r

      if (line) {
        // Verificar se a linha não está vazia
        const [
          L_ORDERKEY,
          L_PARTKEY,
          L_SUPPKEY,
          L_LINENUMBER,
          L_QUANTITY,
          L_EXTENDEDPRICE,
          L_DISCOUNT,
          L_TAX,
          L_RETURNFLAG,
          L_LINESTATUS,
          L_SHIPDATE,
          L_COMMITDATE,
          L_RECEIPTDATE,
          L_SHIPINSTRUCT,
          L_SHIPMODE,
          L_COMMENT,
        ] = line.split("|").map((item) => item.trim());

        const key = `${L_ORDERKEY}_${L_LINENUMBER}`;

        // Criando o JSON
        const jsonValue = JSON.stringify({
            L_PARTKEY: L_PARTKEY,
            L_SUPPKEY: L_SUPPKEY,
            L_QUANTITY: L_QUANTITY,
            L_EXTENDEDPRICE: L_EXTENDEDPRICE,
            L_DISCOUNT: L_DISCOUNT,
            L_TAX: L_TAX,
            L_RETURNFLAG: L_RETURNFLAG,
            L_LINESTATUS: L_LINESTATUS,
            L_SHIPDATE: L_SHIPDATE,
            L_COMMITDATE: L_COMMITDATE,
            L_RECEIPTDATE: L_RECEIPTDATE,
            L_SHIPINSTRUCT: L_SHIPINSTRUCT,
            L_SHIPMODE: L_SHIPMODE,
            L_COMMENT: L_COMMENT
        });

        if (key && jsonValue) {
          // Verificar se todos os campos estão presentes
          try {
            // Inserindo no banco de dados
            await connection.execute(
              `INSERT INTO LINEITEM (L_LINEKEY, L_LINEVALUE) VALUES (:key, :value)`,
              { key: key, value: jsonValue } // Certifique-se de que o key não seja NULL
            );

            console.log(`Inserido: ${key} -> ${jsonValue}`);
          } catch (error) {
            console.error(`Erro ao inserir ${key}: ${error}`);
          }
        } else {
          console.error(`Linha inválida ou dados ausentes: ${line}`);
        }
      }
    }

    // Confirma as inserções
    await connection.commit();
    console.log("Todas as inserções foram confirmadas.");
  } catch (err) {
    console.error("Erro ao conectar ao banco de dados", err);
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log("Conexão fechada");
      } catch (err) {
        console.error("Erro ao fechar a conexão", err);
      }
    }
  }
}

// Executando a função
insertData();
