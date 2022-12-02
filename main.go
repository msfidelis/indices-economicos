package main

import (
	"crawlers/pkg/logger"
	// "crawlers/runners/igpm"
	// "crawlers/runners/inpc"
	// "crawlers/runners/ipca"
	// "crawlers/runners/pib"
	// "crawlers/runners/selic"
	"crawlers/runners/gini"
)

func main() {
	l := logger.Instance()
	l.Info().
		Msg("Iniciando o processo de Crawling dos dados abertos")

	// // PIB
	// l.Info().
	// 	Msg("Iniciando o Runner de PIB")

	// pib.Runner()

	// // IPCA
	// l.Info().
	// 	Msg("Iniciando o Runner de IPCA")

	// ipca.Runner()

	// // INPC
	// l.Info().
	// 	Msg("Iniciando o Runner de INPC")

	// inpc.Runner()

	// // IGP-M
	// l.Info().
	// 	Msg("Iniciando o Runner de IGP-M")

	// igpm.Runner()

	// // SELIC
	// l.Info().
	// 	Msg("Iniciando o Runner de Selic")

	// selic.Runner()

	// GINI
	l.Info().
		Msg("Iniciando o Runner de coeficiente de Gini")

	gini.Runner()

}
