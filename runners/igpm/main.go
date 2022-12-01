package igpm

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocolly/colly"
)

type Data struct {
	MesAno       string  `json:"mes_ano"`
	VariacaoMes  float64 `json:"variacao_mes"`
	AcumuladoAno float64 `json:"acumulado_ano"`
}

type IGPM struct {
	Atualizacao time.Time `json:"data_atualizacao"`
	Fonte       string    `json:"fonte"`
	Data        []Data    `json:"data"`
}

func Runner() {
	runnerName := "IGP-M"
	domain := "informederendimentos.com"
	url := "https://informederendimentos.com/indice-de-preco/igpm/"
	file_path := "./data/igpm/igpm.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	igpm := &IGPM{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	igpm.Atualizacao = now
	igpm.Fonte = url

	// Find and print all links
	c.OnHTML("table", func(e *colly.HTMLElement) {

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Recuperando o HTML da Página")

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Encontrando o elemento <table> para efetuar o parsing")

		e.ForEach("tr", func(i int, tr *colly.HTMLElement) {

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Msg("Item recuparado itens do elemento <table>")

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Msg("Normalizando os dados recuperados")

			mes_referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), ",", ".", -1)
			variacao_mes_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)
			acumulado_ano_td := strings.Replace(tr.ChildText("td:nth-child(3)"), ",", ".", -1)

			variacao_mes, _ := strconv.ParseFloat(strings.TrimSpace(variacao_mes_td), 64)
			acumulado_ano, _ := strconv.ParseFloat(strings.TrimSpace(acumulado_ano_td), 64)

			l.Info().
				Str("Runner", runnerName).
				Msg("Dados normalizados")

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Str("Mês Referência", mes_referencia_td).
				Float64("Variação de Valor Mês", variacao_mes).
				Float64("Valor Acumulado do Ano", acumulado_ano).
				Msg("Item recuparado da linha da tabela")

			item := Data{
				MesAno:       mes_referencia_td,
				VariacaoMes:  variacao_mes,
				AcumuladoAno: acumulado_ano,
			}

			if i == 0 {
				l.Info().
					Str("Runner", runnerName).
					Msg("Descartando os headers da tabela")
			} else {
				l.Info().
					Str("Runner", runnerName).
					Msg("Agregando Item ao Slice")

				igpm.Data = append(igpm.Data, item)

				l.Info().
					Str("Runner", runnerName).
					Msg("Item Agregado ao Slice")

			}

		})

		l.Info().
			Str("Runner", runnerName).
			Msg("Convertendo a Struct do Schema em formato JSON")

		b, err := json.Marshal(igpm)
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

	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)

}
