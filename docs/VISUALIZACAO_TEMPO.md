# Visão total por H3 e últimos N minutos

O mapa inicia em **todo o histórico**: observações de diferentes dias e horários são somadas pelo mesmo hash H3. O filtro de satélite continua disponível. Marque **Limitar tempo de exibição** e informe **Últimos N minutos** para usar uma janela móvel, calculada a cada consulta a partir do horário UTC do servidor. O limite de minutos é de 1 a 525600 (um ano).

A referência é o horário da **observação INPE**, não o momento de chegada ao SQS. Uma observação antiga que chega agora aumenta o total, mas só aparece no modo recente se seu timestamp estiver dentro da janela. A janela inclui os limites inferior e superior; observações futuras não entram no modo recente. Quando uma observação sai do período, desaparece da contagem na próxima atualização bem-sucedida. Em falha de rede, o último resultado é mantido com aviso de dados possivelmente desatualizados.

O consumidor continua persistindo os grupos de dez minutos. Para a visualização, a API consulta as observações deduplicadas `EVENT#...` que já existem no DynamoDB. Isso preserva a precisão de N minutos: somar buckets inteiros daria resultados incorretos na borda, por exemplo nos últimos 7 minutos. A API agrega cada página por H3/satélite; o frontend soma as células entre todas as páginas e satélites escolhidos. O mesmo hash produz uma única célula no mapa. Contadores de focos, células e satélites usam o mesmo conjunto filtrado.

## 1. Adicionar o índice à tabela existente

Console DynamoDB → Tabelas → **inpe-focos-clusters** → Índices → Criar índice:

| Campo | Valor |
|---|---|
| Nome | `ObservationsByTime` |
| Chave de partição | `sk`, String |
| Chave de classificação | `date`, String |
| Projeção de atributos | **Todos / ALL** |

Use capacidade sob demanda, como a tabela. Aguarde o índice ficar **ACTIVE** antes de usar a versão nova da API. O DynamoDB preenche o índice com os itens existentes automaticamente: não é necessário reprocessar CSVs nem modificar o consumidor.

A chave de partição da consulta é `RES#5#WINDOW10M` (ou a resolução configurada). Somente os marcadores de observações correspondem a essa chave; agregados diários/dez minutos não são somados novamente. O índice é eventualmente consistente: novas detecções podem demorar um pouco para aparecer, mesmo depois da gravação. Não se usa `Scan`.

Alternativa pelo CloudShell (execute uma única vez se o índice ainda não existir):

```bash
aws dynamodb update-table \
  --table-name inpe-focos-clusters \
  --attribute-definitions AttributeName=sk,AttributeType=S AttributeName=date,AttributeType=S \
  --global-secondary-index-updates '[{"Create":{"IndexName":"ObservationsByTime","KeySchema":[{"AttributeName":"sk","KeyType":"HASH"},{"AttributeName":"date","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}}]' \
  --region sa-east-1
```

Verifique o status:

```bash
aws dynamodb describe-table --table-name inpe-focos-clusters \
  --query 'Table.GlobalSecondaryIndexes[?IndexName==`ObservationsByTime`].IndexStatus' \
  --region sa-east-1
```

## 2. Atualizar a permissão da Lambda de consulta

IAM → Funções → **inpe-focos-api-role** → política **inpe-focos-api-read** → Editar JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "dynamodb:Query",
    "Resource": [
      "arn:aws:dynamodb:sa-east-1:053109836747:table/inpe-focos-clusters",
      "arn:aws:dynamodb:sa-east-1:053109836747:table/inpe-focos-clusters/index/ObservationsByTime"
    ]
  }]
}
```

A role continua sem gravação e sem Scan. Não altere o usuário produtor nem a role da Lambda consumidora.

## 3. Atualizar a Lambda de consulta

Baixe o novo `artifacts/inpe-api-source.zip`, envie pelo CloudShell → Actions → Upload file e execute:

```bash
unzip -o ~/inpe-api-source.zip -d ~/inpe-api-build
bash ~/inpe-api-build/scripts/build-backend-zip.sh
```

Baixe `/home/cloudshell-user/inpe-api-build/dist/inpe-api.zip` e envie em Lambda **inpe-focos-api** → Code → Upload from → .zip file. O handler permanece `backend.handler.handler`, Python 3.12/x86_64. Variáveis `TABLE_NAME` e `H3_RESOLUTION` e a Function URL permanecem as mesmas.

Somente a Lambda de consulta precisa de novo código. O consumidor e o produtor não precisam de atualização nem replay para essa funcionalidade.

## 4. Abrir o frontend atualizado

Na sua máquina:

```bash
cd /home/italan/Desktop/PROJETO_AIS
python3 -m http.server 8000 --directory frontend
```

Abra `http://localhost:8000` e recarregue com Ctrl+Shift+R. A Function URL já está em `frontend/config.js`. O CORS deve permitir essa origem. Para hospedagem estática, publique a pasta `frontend/` inteira, incluindo o novo `data.js`, e adicione a origem publicada ao CORS.

## API

```text
GET /api/clusters?view=all
GET /api/clusters?view=recent&minutes=60
GET /api/clusters?view=recent&minutes=7&satellite=GOES-19
```

Resposta: FeatureCollection com contagens por hash/satélite, `reference_time`, `cutoff` e `next_cursor`. Cada página é limitada a 1000 observações avaliadas. Siga `next_cursor` usando o mesmo `reference_time` como parâmetro `as_of`, preservando filtros e minutos. Assim o limite temporal não muda durante a paginação. O frontend já faz isso automaticamente. Páginas vazias ainda podem ter cursor por causa de filtros; não interrompa somente porque `features` veio vazio.

As rotas antigas com `day=YYYY-MM-DD` e `window=HH:MM` permanecem disponíveis por compatibilidade, mas a interface não depende mais de dia ou intervalo fixo.

## Volume e custos

O modo total consulta todas as observações do histórico a cada atualização (em páginas), usando o GSI. É simples para a atividade, mas o custo e o tempo de transferência crescem com o dataset. Os resultados são agregados por página e mesclados no navegador. Para históricos muito grandes, uma evolução seria manter totais materializados por H3 e atualizar o cliente incrementalmente. A resposta completa não é um snapshot transacional de todas as páginas; alterações concorrentes convergem na próxima atualização. A aplicação atual mantém o resultado anterior enquanto uma rodada é carregada, sem somar a mesma rodada novamente.
