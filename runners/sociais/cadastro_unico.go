package sociais

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type DataPobreza struct {
	Referencia string `json:"referencia" csv:"referencia"`
	Total      int64  `json:"total" csv:"total"`
}

type Pobreza struct {
	Atualizacao   time.Time     `json:"data_atualizacao"`
	Fonte         string        `json:"fonte"`
	UnidadeMedida string        `json:"unidade_medida"`
	Data          []DataPobreza `json:"data"`
}

type DataPobrezaConsolidado struct {
	Referencia     string `json:"referencia" csv:"referencia"`
	Periodo        int64  `json:"periodo" csv:"periodo"`
	Pobreza        int64  `json:"pobreza" csv:"pobreza"`
	ExtremaPobreza int64  `json:"extrema_pobreza" csv:"extrema_pobreza"`
	Total          int64  `json:"total" csv:"total"`
}

type PobrezaConsolidado struct {
	Atualizacao   time.Time                `json:"data_atualizacao"`
	Fonte         string                   `json:"fonte"`
	UnidadeMedida string                   `json:"unidade_medida"`
	Data          []DataPobrezaConsolidado `json:"data"`
}

func RunnerConsolidacaoPobreza() {
	l := logger.Instance()

	consolidado := make(map[string]DataPobrezaConsolidado)

	runnerName := "Consolidação Pobreza Cadastro Unico"
	pobrezaFile := "./data/sociais/pobreza_cadastro_unico.json"
	extremaPobrezaFile := "./data/sociais/extrema_pobreza_cadastro_unico.json"
	pobrezaExtremaPobrezaFile := "./data/sociais/pobreza_extrema_pobreza_cadastro_unico.json"

	file_path := "./data/sociais/indices_pobreza_consolidado.json"
	fileNameOutputCSV := "./data/sociais/indices_pobreza_consolidado.csv"

	fonte := "https://aplicacoes.cidadania.gov.br"

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Processo de Consolidação de dados")

	indice := PobrezaConsolidado{}
	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = fonte

	l.Info().
		Str("Runner", runnerName).
		Msg("Abrindo os arquivos gerados pelos runners")

	// Pobreza
	dataPobreza := Pobreza{}
	filePobreza, err := ioutil.ReadFile(pobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePobreza), &dataPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataPobreza.Data {
		splitData := strings.Split(k.Referencia, "/")
		dataRaw := fmt.Sprintf("%v%v", splitData[1], splitData[0])
		periodo, _ := strconv.ParseInt(dataRaw, 10, 64)

		item := DataPobrezaConsolidado{
			Periodo:    periodo,
			Referencia: k.Referencia,
			Pobreza:    k.Total,
		}

		consolidado[k.Referencia] = item

	}

	// Extrema Pobreza
	dataExtremaPobreza := Pobreza{}
	fileExtremaPobreza, err := ioutil.ReadFile(extremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", extremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileExtremaPobreza), &dataExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", extremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.ExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Pobreza / Extrema Pobreza
	dataPobrezaExtremaPobreza := Pobreza{}
	filePobrezaExtremaPobreza, err := ioutil.ReadFile(pobrezaExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePobrezaExtremaPobreza), &dataPobrezaExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataPobrezaExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.Total = k.Total
		consolidado[k.Referencia] = item
	}

	for _, k := range consolidado {
		indice.Data = append(indice.Data, k)
	}

	// Sort do data
	sort.Slice(indice.Data, func(i, j int) bool {
		return indice.Data[i].Periodo < indice.Data[j].Periodo
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(indice)
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

	csvOutput, err := gocsv.MarshalString(&indice.Data)
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

	fmt.Println(consolidado)

}
