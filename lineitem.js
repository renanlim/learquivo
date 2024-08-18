const fs = require("fs");
const oracledb = require("oracledb");

async function insertData() {
  let connection;

  try {
    // Conectando ao banco de dados Oracle
    connection = await oracledb.getConnection({
      user: "C##TESTE",
      password: "rei9122947", // Substitua pela sua senha
      connectString: "majovdev-pc2:1521/xe",
    });

    console.log("Conectado ao Oracle Database");

    // Lendo o arquivo
    const data = fs.readFileSync(
      "D:\\Users\\Renan Lima\\Documents\\Projetos Pessoais\\learquivo\\lineitem.tbl",
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
          order_key,
          part_key,
          supp_id,
          line_number,
          quantity,
          extended_price,
          discount,
          tax,
          return_flag,
          status,
          ship_date,
          commit_date,
          receipt_date,
          ship_instructions,
          ship_mode,
        ] = line.split("|").map((item) => item.trim());

        const key = `${order_key}_${part_key}_${line_number}`;

        // Criando o JSON
        const jsonValue = JSON.stringify({
            supp_id: supp_id,
            quantity: quantity,
            extended_price: extended_price,
            discount: discount,
            tax: tax,
            return_flag: return_flag,
            status: status,
            ship_date: ship_date,
            commit_date: commit_date,
            receipt_date: receipt_date,
            ship_instructions: ship_instructions,
            ship_mode: ship_mode
        });

        if (key && jsonValue) {
          // Verificar se todos os campos estão presentes
          try {
            // Inserindo no banco de dados
            await connection.execute(
              `INSERT INTO LINEITEM (key, value) VALUES (:key, :value)`,
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
