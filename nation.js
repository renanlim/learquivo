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
        const data = fs.readFileSync('D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\nation.tbl', 'utf8');
        const lines = data.split('\n');

        // Iterando sobre cada linha do arquivo
        for (let line of lines) {
            // Limpar espaços em branco e caracteres de nova linha
            line = line.trim().replace(/\r/g, ''); // Remove caracteres \r

            if (line) { // Verificar se a linha não está vazia
                const [N_NATIONKEY, ...values] = line.split('|');
                const N_NAME = values[0]?.trim();
                const N_REGIONKEY = values[1]?.trim();
                let N_COMMENT = values.slice(2).join('|')?.trim(); // Reunir os valores separados por | e limpar espaços
                
                // Remove o caractere | no final da descrição, se houver
                N_COMMENT = N_COMMENT.replace(/\|$/, '');

                if (N_NATIONKEY && N_NAME && N_REGIONKEY && N_COMMENT) { // Verificar se todos os campos estão presentes
                    // Criando o JSON
                    const jsonValue = JSON.stringify({
                        N_NAME: N_NAME,
                        N_REGIONKEY: N_REGIONKEY,
                        N_COMMENT: N_COMMENT
                    });

                    try {
                        // Inserindo no banco de dados
                        const result = await connection.execute(
                            `INSERT INTO NATION (N_NATIONKEY, N_NATIONVALUES) VALUES (:key, :value)`,
                            { key: N_NATIONKEY.trim(), value: jsonValue } // Certifique-se de que o key não seja NULL
                        );

                        console.log(`Inserido: ${N_NATIONKEY.trim()} -> ${jsonValue}`);
                    } catch (error) {
                        console.error(`Erro ao inserir ${N_NATIONKEY.trim()}: ${error}`);
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
