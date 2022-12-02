package pib

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

type ResponseAcumulado []struct {
	NtCod                 string `json:"nt_cod"`
	Nt                    string `json:"nt"`
	UgCod                 string `json:"ug_cod"`
	Ug                    string `json:"ug"`
	PCod                  string `json:"p_cod"`
	P                     string `json:"p"`
	VarCod                string `json:"var_cod"`
	Var                   string `json:"var"`
	SetoresESubsetoresCod string `json:"setores_e_subsetores_cod"`
	SetoresESubsetores    string `json:"setores_e_subsetores"`
	UmCod                 string `json:"um_cod"`
	Um                    string `json:"um"`
	V                     string `json:"v"`
}

type DataAcumulado struct {
	Periodo string  `json:"periodo"`
	Valor   float64 `json:"valor"`
}

type PIBAcumulado struct {
	Atualizacao   time.Time       `json:"data_atualizacao"`
	UnidadeMedida string          `json:"unidade_medida"`
	Fonte         string          `json:"fonte"`
	Data          []DataAcumulado `json:"data"`
}

func RunnerPIBAcumulado() {
	runnerName := "PIB - Acumulado"
	url := "https://servicodados.ibge.gov.br/api/v1/conjunturais?&d=s&user=ibge&t=1846&v=585&p=200001-205701&ng=1(1)&c=11255(90707)"
	unidadeMedida := "Milhões de Reais"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/pib/pib-acumulado.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	pib := &PIBAcumulado{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	pib.Atualizacao = now
	pib.Fonte = fonte
	pib.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do IBGE para recuperar a série histórica do pib")

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTP para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response ResponseAcumulado

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, v := range response {
		fmt.Println(v)

		valor, err := strconv.ParseFloat(strings.TrimSpace(v.V), 64)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", v.V).
				Msg("Erro ao converter o valor para Float64")
		}

		item := DataAcumulado{
			Periodo: v.P,
			Valor:   valor,
		}

		l.Info().
			Str("Runner", runnerName).
			Msg("Agregando Item ao Slice")

		pib.Data = append(pib.Data, item)

		l.Info().
			Str("Runner", runnerName).
			Msg("Item Agregado ao Slice")
	}

	// Recupera o nome dos campos da Struct onde ficam os valores do PIB
	// names := structs.Names(response[0].Resultados[0].Series[0].Serie)

	// for _, v := range names {

	// 	fmt.Println(v)
	// 	r := reflect.ValueOf(response[0].Resultados[0].Series[0].Serie)
	// 	f := reflect.Indirect(r).FieldByName(v)

	// 	valueRaw := f.String()

	// 	fmt.Println(valueRaw)

	// 	l.Info().
	// 		Str("Runner", runnerName).
	// 		Str("Valor recuperado", valueRaw).
	// 		Msg("Convertendo valor recuperado da pib para Float64")

	// 	if valueRaw == "" {
	// 		l.Warn().
	// 			Str("Runner", runnerName).
	// 			Str("Valor recuperado", valueRaw).
	// 			Msg("Ignorando o valor recuperado pois inda não foi contabilizado pela fonte")
	// 		continue
	// 	}

	// 	valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

	// 	if err != nil {
	// 		l.Fatal().
	// 			Str("Runner", runnerName).
	// 			Str("Error", err.Error()).
	// 			Str("Valor recuperado", valueRaw).
	// 			Msg("Erro ao converter o valor para Float64")
	// 	}

	// 	l.Info().
	// 		Str("Runner", runnerName).
	// 		Str("Data recuperada", v).
	// 		Msg("Removendo patterns desnecessários da string")

	// 	ano := strings.Replace(v, "Num", "", -1)

	// 	l.Info().
	// 		Str("Runner", runnerName).
	// 		Str("Data recuperada", v).
	// 		Str("Data sem os patterns removidos", ano).
	// 		Msg("Patterns removidos da string")

	// 	item := DataPerCapta{
	// 		Ano:   ano,
	// 		Valor: valor,
	// 	}

	// 	l.Info().
	// 		Str("Runner", runnerName).
	// 		Msg("Agregando Item ao Slice")

	// 	pib.Data = append(pib.Data, item)

	// 	l.Info().
	// 		Str("Runner", runnerName).
	// 		Msg("Item Agregado ao Slice")
	// }

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(pib)
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
