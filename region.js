const fs = require('fs');
const oracledb = require('oracledb');

async function insertData() {
    let connection;

    try {
        // Conectando ao banco de dados Oracle
        connection = await oracledb.getConnection({
            user: 'C##TESTE',
            password: 'rei9122947',
            connectString: 'majovdev-pc2:1521/xe'
        });

        console.log('Conectado ao Oracle Database');

        // Lendo o arquivo
        const data = fs.readFileSync('D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\region.tbl', 'utf8');
        const lines = data.split('\n');

        // Ordenar as linhas por algum critério se necessário (aqui estamos apenas processando na ordem original)
        const sortedLines = lines.sort();

        // Iterando sobre cada linha do arquivo
        for (let line of sortedLines) {
            // Limpar espaços em branco e caracteres de nova linha
            line = line.trim().replace(/\r/g, ''); // Remove caracteres \r

            if (line) { // Verificar se a linha não está vazia
                const [R_REGIONKEY, R_NAME, R_COMMENT] = line.split('|').map(item => item.trim());

                // Criando o JSON
                const jsonValue = JSON.stringify({
                    R_NAME: R_NAME,
                    R_COMMENT: R_COMMENT
                });

                if (R_REGIONKEY && jsonValue) { // Verificar se todos os campos estão presentes
                    try {
                        // Inserindo no banco de dados
                        const result = await connection.execute(
                            `INSERT INTO REGION (R_REGIONKEY, R_REGIONVALUE) VALUES (:key, :value)`,
                            { key: R_REGIONKEY, value: jsonValue } // Certifique-se de que o key não seja NULL
                        );

                        console.log(`Inserido: ${R_REGIONKEY} -> ${jsonValue}`);
                    } catch (error) {
                        console.error(`Erro ao inserir ${R_REGIONKEY}: ${error}`);
                    }
                } else {
                    console.error(`Linha inválida ou dados ausentes: ${line}`);
                }
            }
        }

        // Confirma as inserções
        await connection.commit();
        console.log('Todas as inserções foram confirmadas.');

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
