const fs = require('fs');
const oracledb = require('oracledb');

async function insertData() {
    let connection;

    try {
        // Conectando ao banco de dados Oracle
        connection = await oracledb.getConnection({
            user: 'C##TESTE',
            password: 'rei9122947', // Substitua pela sua senha
            connectString: 'majovdev-pc2:1521/xe'
        });

        console.log('Conectado ao Oracle Database');

        // Lendo o arquivo
        const data = fs.readFileSync('D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\supplier.tbl', 'utf8');
        const lines = data.split('\n');

        // Ordenar as linhas por algum critério se necessário (aqui estamos apenas processando na ordem original)
        const sortedLines = lines.sort();

        // Iterando sobre cada linha do arquivo
        for (let line of sortedLines) {
            // Limpar espaços em branco e caracteres de nova linha
            line = line.trim().replace(/\r/g, ''); // Remove caracteres \r

            if (line) { // Verificar se a linha não está vazia
                const [S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, S_ACCTBAL, S_COMMENT] = line.split('|').map(item => item.trim());

                // Criando o JSON
                const jsonValue = JSON.stringify({
                    S_NAME: S_NAME,
                    S_ADDRESS: S_ADDRESS,
                    S_PHONE: S_PHONE,
                    S_ACCTBAL: S_ACCTBAL,
                    S_COMMENT: S_COMMENT
                });

                if (S_SUPPKEY && jsonValue) { // Verificar se todos os campos estão presentes
                    try {
                        // Inserindo no banco de dados
                        await connection.execute(
                            `INSERT INTO SUPPLIER (S_SUPPKEY, S_SUPPVALUE) VALUES (:key, :value)`,
                            { key: S_SUPPKEY, value: jsonValue } // Certifique-se de que o key não seja NULL
                        );

                        console.log(`Inserido: ${S_SUPPKEY} -> ${jsonValue}`);
                    } catch (error) {
                        console.error(`Erro ao inserir ${S_SUPPKEY}: ${error}`);
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
