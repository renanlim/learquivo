const fs = require('fs');
const oracledb = require('oracledb');
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
            connectString: BD_CONNECT
        });

        console.log('Conectado ao Oracle Database');

        // Lendo o arquivo
        const data = fs.readFileSync('D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\files\\part.tbl', 'utf8');
        const lines = data.split('\n');

        // Iterando sobre cada linha do arquivo
        for (let line of lines) {
            // Limpar espaços em branco e caracteres de nova linha
            line = line.trim().replace(/\r/g, ''); // Remove caracteres \r

            if (line) { // Verificar se a linha não está vazia
                const [P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT] = line.split('|').map(item => item.trim());

                // Criando o JSON
                const jsonValue = JSON.stringify({
                    P_NAME: P_NAME,
                    P_MFGR: P_MFGR,
                    P_BRAND: P_BRAND,
                    P_TYPE: P_TYPE,
                    P_SIZE: P_SIZE,
                    P_CONTAINER: P_CONTAINER,
                    P_RETAILPRICE: P_RETAILPRICE,
                    P_COMMENT: P_COMMENT
                });

                if (P_PARTKEY && jsonValue) { // Verificar se todos os campos estão presentes
                    try {
                        // Inserindo no banco de dados
                        const result = await connection.execute(
                            `INSERT INTO PART (P_PARTKEY, P_PARTVALUE) VALUES (:key, :value)`,
                            { key: P_PARTKEY, value: jsonValue } // Certifique-se de que o key não seja NULL
                        );

                        console.log(`Inserido: ${P_PARTKEY} -> ${jsonValue}`);
                    } catch (error) {
                        console.error(`Erro ao inserir ${P_PARTKEY}: ${error}`);
                    }
                } else {
                    console.error(`Linha inválida ou dados ausentes: ${line}`);
                }
            }
        }

        // Confirma as inserções
        await connection.commit();

    } catch (err) {
        console.error('Erro ao conectar ao banco de dados', err);
    } finally {
        if (connection) {
            try {
                await connection.close();
                console.log('Conexão fechada');
            } catch (err) {
                console.error('Erro ao fechar a conexão', err);
            }
        }
    }
}

// Executando a função
insertData();
