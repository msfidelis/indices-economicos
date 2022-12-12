package inflacao

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/gocarina/gocsv"
)

type Data struct {
	Referencia           string  `json:"referencia" csv:"referencia"`
	Ano                  string  `json:"ano" csv:"ano"`
	Mes                  string  `json:"mes" csv:"mes"`
	IPCAVariacao         float64 `json:"ipca_variacao" csv:"ipca_variacao"`
	IPCAAcumuladoAno     float64 `json:"ipca_acumulado_ano" csv:"ipca_acumulado_ano"`
	IPCAAcumulado12Meses float64 `json:"ipca_acumulado_doze_meses" csv:"ipca_acumulado_doze_meses"`
	INPCVariacao         float64 `json:"inpc_variacao" csv:"inpc_variacao"`
	INPCAcumuladoAno     float64 `json:"inpc_acumulado_ano" csv:"inpc_acumulado_ano"`
	INPCAcumulado12Meses float64 `json:"inpc_acumulado_doze_meses" csv:"inpc_acumulado_doze_meses"`
	ConsolidacaoAno      bool    `json:"consolidado_ano" csv:"consolidado_ano"`
}

type Inflacao struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func RunnerConsolidacao() {
	l := logger.Instance()

	runnerName := "Inflação - Consolidacao"

	ipcaFile := "./data/inflacao/ipca.json"
	inpcFile := "./data/inflacao/inpc.json"

	file_path := "./data/inflacao/inflacao.json"
	fileNameOutputCSV := "./data/inflacao/inflacao.csv"

	consolidado := make(map[string]Data)

	inflacao := Inflacao{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner Consolidar os Datasets")

	IPCA := IPCA{}
	fileIPCA, err := ioutil.ReadFile(ipcaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCA), &IPCA)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	INPC := INPC{}
	fileINPC, err := ioutil.ReadFile(inpcFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINPC), &INPC)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("converter para struct")
	}

	// Construção do map de referencias

	l.Info().
		Str("Runner", runnerName).
		Msg("Construção do map de referencias")

	for _, ip := range IPCA.Data {

		item := Data{}
		item.Referencia = ip.Referencia
		item.Ano = ip.Ano
		item.Mes = ip.Mes

		if ip.Mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA ao Consolidado")

	for _, ip := range IPCA.Data {

		item := consolidado[ip.Referencia]
		item.IPCAVariacao = ip.Variacao
		item.IPCAAcumuladoAno = ip.AcumuladoAno
		item.IPCAAcumulado12Meses = ip.Acumulado12Meses

		l.Info().
			Str("Runner", runnerName).
			Str("Dataset", "IPCA").
			Str("Periodo", ip.Referencia).
			Msg("Agregando os Item ao Consolidado")

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INPC ao Consolidado")

	for _, in := range INPC.Data {

		item := consolidado[in.Referencia]
		item.INPCVariacao = in.Variacao
		item.INPCAcumuladoAno = in.AcumuladoAno
		item.INPCAcumulado12Meses = in.Acumulado12Meses

		l.Info().
			Str("Runner", runnerName).
			Str("Dataset", "INPC").
			Str("Periodo", in.Referencia).
			Msg("Agregando os Item ao Consolidado")

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Criando o Struct Final de Inflação")

	for _, i := range consolidado {
		if i.Referencia != "" {
			inflacao.Data = append(inflacao.Data, i)
		}
	}

	// Sort do data
	sort.Slice(inflacao.Data, func(i, j int) bool {
		return inflacao.Data[i].Referencia < inflacao.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(inflacao)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Criando arquivo de persistência para os dados convertidos")

	f, err := os.Create(file_path)
	defer f.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro ao criar o diretório para persistência dos dados")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = f.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Convertendo em CSV
	csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFile.Close()

	csvOutput, err := gocsv.MarshalString(&inflacao.Data)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o dataset em CSV")
	}

	_, err = csvFile.WriteString(string(csvOutput))
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")

}
