package selic

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Response []struct {
	Data  string `json:"data"`
	Valor string `json:"valor"`
}

type Data struct {
	MesAno string  `json:"mes_ano"`
	Valor  float64 `json:"valor"`
}

type SELIC struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func Runner() {
	RunnerAcumuladoMensal()
	RunnerPercentualAoAno()
}

func RunnerAcumuladoMensal() {
	runnerName := "SELIC"
	// domain := "api.bcb.gov.br"
	url := "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados/ultimos/1000?formato=json"
	unidadeMedida := "Percentual ao mês"

	file_path := "./data/selic/selic-variacao-mes.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	selic := &SELIC{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	selic.Atualizacao = now
	selic.Fonte = url
	selic.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do bcbdata para recuperar a série histórica da SELIC")

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTO para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response Response

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, d := range response {

		l.Info().
			Str("Runner", runnerName).
			Str("Valor recuperado", d.Valor).
			Msg("Convertendo valor recuperado da SELIC para Float64")

		valor, err := strconv.ParseFloat(strings.TrimSpace(d.Valor), 64)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", d.Valor).
				Msg("Erro ao converter o valor para Float64")
		}

		// Formatando data
		// Convertendo para os formatos aceitos pelo Go
		// Neste momento vindo como dd/mm/yyyy, convertendo para yyyy/mm/dd por string

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperado", d.Data).
			Msg("Convertendo a data para o formato Jan/06")

		splitData := strings.Split(d.Data, "/")
		dataRebuilded := fmt.Sprintf("%s/%s/%s", splitData[2], splitData[1], splitData[0])

		layout := "2006/01/02"
		t, _ := time.Parse(layout, dataRebuilded)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Data recuperada", d.Data).
				Str("Data reconstruida", dataRebuilded).
				Str("Layout", layout).
				Msg("Erro ao converter o Layout de data")
		}

		formatedDate := t.Format("Jan/06")

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", d.Data).
			Str("Data transformada", formatedDate).
			Msg("Data recuperada convertida com sucesso")

		item := Data{
			MesAno: formatedDate,
			Valor:  valor,
		}

		l.Info().
			Str("Runner", runnerName).
			Msg("Agregando Item ao Slice")

		selic.Data = append(selic.Data, item)

		l.Info().
			Str("Runner", runnerName).
			Msg("Item Agregado ao Slice")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(selic)
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

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")
}

func RunnerPercentualAoAno() {

	runnerName := "SELIC"
	// domain := "api.bcb.gov.br"
	url := "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4189/dados/ultimos/1000?formato=json"
	unidadeMedida := "Percentual ao ano"

	file_path := "./data/selic/selic-percentual-ano.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	selic := &SELIC{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	selic.Atualizacao = now
	selic.Fonte = url
	selic.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do bcbdata para recuperar a série histórica da SELIC")

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTO para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response Response

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, d := range response {

		l.Info().
			Str("Runner", runnerName).
			Str("Valor recuperado", d.Valor).
			Msg("Convertendo valor recuperado da SELIC para Float64")

		valor, err := strconv.ParseFloat(strings.TrimSpace(d.Valor), 64)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", d.Valor).
				Msg("Erro ao converter o valor para Float64")
		}

		// Formatando data
		// Convertendo para os formatos aceitos pelo Go
		// Neste momento vindo como dd/mm/yyyy, convertendo para yyyy/mm/dd por string

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperado", d.Data).
			Msg("Convertendo a data para o formato Jan/06")

		splitData := strings.Split(d.Data, "/")
		dataRebuilded := fmt.Sprintf("%s/%s/%s", splitData[2], splitData[1], splitData[0])

		layout := "2006/01/02"
		t, _ := time.Parse(layout, dataRebuilded)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Data recuperada", d.Data).
				Str("Data reconstruida", dataRebuilded).
				Str("Layout", layout).
				Msg("Erro ao converter o Layout de data")
		}

		formatedDate := t.Format("Jan/06")

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", d.Data).
			Str("Data transformada", formatedDate).
			Msg("Data recuperada convertida com sucesso")

		item := Data{
			MesAno: formatedDate,
			Valor:  valor,
		}

		l.Info().
			Str("Runner", runnerName).
			Msg("Agregando Item ao Slice")

		selic.Data = append(selic.Data, item)

		l.Info().
			Str("Runner", runnerName).
			Msg("Item Agregado ao Slice")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(selic)
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

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")

}
