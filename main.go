package main

import (
	"crawlers/pkg/logger"
	"crawlers/runners/igpm"
	"crawlers/runners/inpc"
	"crawlers/runners/ipca"
	"crawlers/runners/pib"
	"crawlers/runners/selic"
)

type Data struct {
	MesAno       string  `json:"mes_ano"`
	VariacaoMes  float64 `json:"variacao_mes"`
	AcumuladoAno float64 `json:"acumulado_ano"`
}

type IPCA struct {
	Data []Data `json:"data"`
}

func main() {
	l := logger.Instance()
	l.Info().
		Msg("Iniciando o processo de Crawling dos dados abertos")

	// PIB
	l.Info().
		Msg("Iniciando o Runner de PIB")

	pib.Runner()

	// IPCA
	l.Info().
		Msg("Iniciando o Runner de IPCA")

	ipca.Runner()

	// INPC
	l.Info().
		Msg("Iniciando o Runner de INPC")

	inpc.Runner()

	// IGP-M
	l.Info().
		Msg("Iniciando o Runner de IGP-M")

	igpm.Runner()

	l.Info().
		Msg("Iniciando o Runner de Selic")

	selic.Runner()

}
