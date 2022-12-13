package inflacao

import (
	"crawlers/pkg/logger"
	"fmt"
	"time"

	"github.com/gocolly/colly"
)

type DataINCC struct {
	Referencia        string  `json:"referencia" csv:"referencia"`
	Ano               string  `json:"ano" csv:"ano"`
	Mes               string  `json:"mes" csv:"mes"`
	Variacao          float64 `json:"variacao" csv:"variacao"`
	AcumuladoAno      float64 `json:"acumulado_ano" csv:"acumulado_ano"`
	Acumulado12Meses  float64 `json:"acumulado_doze_meses" csv:"acumulado_doze_meses"`
	ConsolidacaoAno   bool    `json:"consolidado_ano" csv:"consolidado_ano"`
	IdentificadorIBGE string  `json:"identificador_ibge" csv:"identificador_ibge"`
}

type INCC struct {
	Atualizacao   time.Time  `json:"data_atualizacao"`
	UnidadeMedida string     `json:"unidade_medida"`
	Fonte         string     `json:"fonte"`
	Data          []DataINCC `json:"data"`
}

func RunnerINCC() {
	runnerName := "INCC-DI"
	domain := "indiceseconomicos.secovi.com.br"
	url := "http://indiceseconomicos.secovi.com.br/indicadormensal.php?idindicador=59"
	// file_path := "./data/inflacao/incc.json"
	// fileNameOutputCSV := "./data/inflacao/incc.csv"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	incc := &INCC{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	incc.Atualizacao = now
	incc.Fonte = url

	// Find and print all links
	c.OnHTML("table", func(e *colly.HTMLElement) {
		fmt.Println(e)
	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)
}
