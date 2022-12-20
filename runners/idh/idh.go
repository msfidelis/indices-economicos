package idh

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

var (
	fileNameOutput string
	fileNameRaw    string
	fullURLFile    string
)

type Data struct {
	Ano                                string  `json:"ano_referencia" csv:"ano_referencia"`
	IDH                                float64 `json:"idh" csv:"idh"`
	IDHF                               float64 `json:"idh_feminino" csv:"idh_feminino"`
	IDHM                               float64 `json:"idh_masculino" csv:"idh_masculino"`
	ExpectativaDeVida                  float64 `json:"expectativa_de_vida" csv:"expectativa_de_vida"`
	ExpectativaDeVidaFeminina          float64 `json:"expectativa_de_vida_feminina" csv:"expectativa_de_vida_feminina"`
	ExpectativaDeVidaMasculina         float64 `json:"expectativa_de_vida_masculina" csv:"expectativa_de_vida_masculina"`
	ExpectativaDeAnosNaEscola          float64 `json:"expectativa_de_anos_escola" csv:"expectativa_de_anos_escola"`
	ExpectativaDeAnosNaEscolaFeminina  float64 `json:"expectativa_de_anos_escola_feminina" csv:"expectativa_de_anos_escola_feminina"`
	ExpectativaDeAnosNaEscolaMasculina float64 `json:"expectativa_de_anos_escola_masculina" csv:"expectativa_de_anos_escola_masculina"`
	MediaDeAnosNaEscola                float64 `json:"media_de_anos_escola" csv:"media_de_anos_escola"`
	MediaDeAnosNaEscolaFeminina        float64 `json:"media_de_anos_escola_feminina" csv:"media_de_anos_escola_feminina"`
	MediaDeAnosNaEscolaMasculina       float64 `json:"media_de_anos_escola_masculina" csv:"media_de_anos_escola_masculina"`
}

type HDI struct {
	Atualizacao time.Time `json:"data_atualizacao"`
	Fonte       string    `json:"fonte"`
	Data        []Data    `json:"data"`
}

type HDICsv struct {
	Pais string `csv:"Country"`

	//HDI
	HDI1991 string `csv:"hdi_1991"`
	HDI1992 string `csv:"hdi_1992"`
	HDI1993 string `csv:"hdi_1993"`
	HDI1994 string `csv:"hdi_1994"`
	HDI1995 string `csv:"hdi_1995"`
	HDI1996 string `csv:"hdi_1996"`
	HDI1997 string `csv:"hdi_1997"`
	HDI1998 string `csv:"hdi_1998"`
	HDI1999 string `csv:"hdi_1999"`
	HDI2000 string `csv:"hdi_2000"`
	HDI2001 string `csv:"hdi_2001"`
	HDI2002 string `csv:"hdi_2002"`
	HDI2003 string `csv:"hdi_2003"`
	HDI2004 string `csv:"hdi_2004"`
	HDI2005 string `csv:"hdi_2005"`
	HDI2006 string `csv:"hdi_2006"`
	HDI2007 string `csv:"hdi_2007"`
	HDI2008 string `csv:"hdi_2008"`
	HDI2009 string `csv:"hdi_2009"`
	HDI2010 string `csv:"hdi_2010"`
	HDI2011 string `csv:"hdi_2011"`
	HDI2012 string `csv:"hdi_2012"`
	HDI2013 string `csv:"hdi_2013"`
	HDI2014 string `csv:"hdi_2014"`
	HDI2015 string `csv:"hdi_2015"`
	HDI2016 string `csv:"hdi_2016"`
	HDI2017 string `csv:"hdi_2017"`
	HDI2018 string `csv:"hdi_2018"`
	HDI2019 string `csv:"hdi_2019"`
	HDI2020 string `csv:"hdi_2020"`
	HDI2021 string `csv:"hdi_2021"`
	HDI2022 string `csv:"hdi_2022"`
	HDI2023 string `csv:"hdi_2023"`

	// IDH Feminino
	FHD1991 string `csv:"hdi_f_1991"`
	FHD1992 string `csv:"hdi_f_1992"`
	FHD1993 string `csv:"hdi_f_1993"`
	FHD1994 string `csv:"hdi_f_1994"`
	FHD1995 string `csv:"hdi_f_1995"`
	FHD1996 string `csv:"hdi_f_1996"`
	FHD1997 string `csv:"hdi_f_1997"`
	FHD1998 string `csv:"hdi_f_1998"`
	FHD1999 string `csv:"hdi_f_1999"`
	FHD2000 string `csv:"hdi_f_2000"`
	FHD2001 string `csv:"hdi_f_2001"`
	FHD2002 string `csv:"hdi_f_2002"`
	FHD2003 string `csv:"hdi_f_2003"`
	FHD2004 string `csv:"hdi_f_2004"`
	FHD2005 string `csv:"hdi_f_2005"`
	FHD2006 string `csv:"hdi_f_2006"`
	FHD2007 string `csv:"hdi_f_2007"`
	FHD2008 string `csv:"hdi_f_2008"`
	FHD2009 string `csv:"hdi_f_2009"`
	FHD2010 string `csv:"hdi_f_2010"`
	FHD2011 string `csv:"hdi_f_2011"`
	FHD2012 string `csv:"hdi_f_2012"`
	FHD2013 string `csv:"hdi_f_2013"`
	FHD2014 string `csv:"hdi_f_2014"`
	FHD2015 string `csv:"hdi_f_2015"`
	FHD2016 string `csv:"hdi_f_2016"`
	FHD2017 string `csv:"hdi_f_2017"`
	FHD2018 string `csv:"hdi_f_2018"`
	FHD2019 string `csv:"hdi_f_2019"`
	FHD2020 string `csv:"hdi_f_2020"`
	FHD2021 string `csv:"hdi_f_2021"`
	FHD2022 string `csv:"hdi_f_2022"`
	FHD2023 string `csv:"hdi_f_2023"`

	// IDH Masculino
	MHD1991 string `csv:"hdi_m_1991"`
	MHD1992 string `csv:"hdi_m_1992"`
	MHD1993 string `csv:"hdi_m_1993"`
	MHD1994 string `csv:"hdi_m_1994"`
	MHD1995 string `csv:"hdi_m_1995"`
	MHD1996 string `csv:"hdi_m_1996"`
	MHD1997 string `csv:"hdi_m_1997"`
	MHD1998 string `csv:"hdi_m_1998"`
	MHD1999 string `csv:"hdi_m_1999"`
	MHD2000 string `csv:"hdi_m_2000"`
	MHD2001 string `csv:"hdi_m_2001"`
	MHD2002 string `csv:"hdi_m_2002"`
	MHD2003 string `csv:"hdi_m_2003"`
	MHD2004 string `csv:"hdi_m_2004"`
	MHD2005 string `csv:"hdi_m_2005"`
	MHD2006 string `csv:"hdi_m_2006"`
	MHD2007 string `csv:"hdi_m_2007"`
	MHD2008 string `csv:"hdi_m_2008"`
	MHD2009 string `csv:"hdi_m_2009"`
	MHD2010 string `csv:"hdi_m_2010"`
	MHD2011 string `csv:"hdi_m_2011"`
	MHD2012 string `csv:"hdi_m_2012"`
	MHD2013 string `csv:"hdi_m_2013"`
	MHD2014 string `csv:"hdi_m_2014"`
	MHD2015 string `csv:"hdi_m_2015"`
	MHD2016 string `csv:"hdi_m_2016"`
	MHD2017 string `csv:"hdi_m_2017"`
	MHD2018 string `csv:"hdi_m_2018"`
	MHD2019 string `csv:"hdi_m_2019"`
	MHD2020 string `csv:"hdi_m_2020"`
	MHD2021 string `csv:"hdi_m_2021"`
	MHD2022 string `csv:"hdi_m_2022"`
	MHD2023 string `csv:"hdi_m_2023"`

	// Expectativa de Vida - Geral
	LEG1991 string `csv:"le_1991"`
	LEG1992 string `csv:"le_1992"`
	LEG1993 string `csv:"le_1993"`
	LEG1994 string `csv:"le_1994"`
	LEG1995 string `csv:"le_1995"`
	LEG1996 string `csv:"le_1996"`
	LEG1997 string `csv:"le_1997"`
	LEG1998 string `csv:"le_1998"`
	LEG1999 string `csv:"le_1999"`
	LEG2000 string `csv:"le_2000"`
	LEG2001 string `csv:"le_2001"`
	LEG2002 string `csv:"le_2002"`
	LEG2003 string `csv:"le_2003"`
	LEG2004 string `csv:"le_2004"`
	LEG2005 string `csv:"le_2005"`
	LEG2006 string `csv:"le_2006"`
	LEG2007 string `csv:"le_2007"`
	LEG2008 string `csv:"le_2008"`
	LEG2009 string `csv:"le_2009"`
	LEG2010 string `csv:"le_2010"`
	LEG2011 string `csv:"le_2011"`
	LEG2012 string `csv:"le_2012"`
	LEG2013 string `csv:"le_2013"`
	LEG2014 string `csv:"le_2014"`
	LEG2015 string `csv:"le_2015"`
	LEG2016 string `csv:"le_2016"`
	LEG2017 string `csv:"le_2017"`
	LEG2018 string `csv:"le_2018"`
	LEG2019 string `csv:"le_2019"`
	LEG2020 string `csv:"le_2020"`
	LEG2021 string `csv:"le_2021"`
	LEG2022 string `csv:"le_2022"`
	LEG2023 string `csv:"le_2023"`

	// Expectativa de Vida - Feminina
	LEF1991 string `csv:"le_f_1991"`
	LEF1992 string `csv:"le_f_1992"`
	LEF1993 string `csv:"le_f_1993"`
	LEF1994 string `csv:"le_f_1994"`
	LEF1995 string `csv:"le_f_1995"`
	LEF1996 string `csv:"le_f_1996"`
	LEF1997 string `csv:"le_f_1997"`
	LEF1998 string `csv:"le_f_1998"`
	LEF1999 string `csv:"le_f_1999"`
	LEF2000 string `csv:"le_f_2000"`
	LEF2001 string `csv:"le_f_2001"`
	LEF2002 string `csv:"le_f_2002"`
	LEF2003 string `csv:"le_f_2003"`
	LEF2004 string `csv:"le_f_2004"`
	LEF2005 string `csv:"le_f_2005"`
	LEF2006 string `csv:"le_f_2006"`
	LEF2007 string `csv:"le_f_2007"`
	LEF2008 string `csv:"le_f_2008"`
	LEF2009 string `csv:"le_f_2009"`
	LEF2010 string `csv:"le_f_2010"`
	LEF2011 string `csv:"le_f_2011"`
	LEF2012 string `csv:"le_f_2012"`
	LEF2013 string `csv:"le_f_2013"`
	LEF2014 string `csv:"le_f_2014"`
	LEF2015 string `csv:"le_f_2015"`
	LEF2016 string `csv:"le_f_2016"`
	LEF2017 string `csv:"le_f_2017"`
	LEF2018 string `csv:"le_f_2018"`
	LEF2019 string `csv:"le_f_2019"`
	LEF2020 string `csv:"le_f_2020"`
	LEF2021 string `csv:"le_f_2021"`
	LEF2022 string `csv:"le_f_2022"`
	LEF2023 string `csv:"le_f_2023"`

	// Expectativa de Vida - Masculina
	LEM1991 string `csv:"le_m_1991"`
	LEM1992 string `csv:"le_m_1992"`
	LEM1993 string `csv:"le_m_1993"`
	LEM1994 string `csv:"le_m_1994"`
	LEM1995 string `csv:"le_m_1995"`
	LEM1996 string `csv:"le_m_1996"`
	LEM1997 string `csv:"le_m_1997"`
	LEM1998 string `csv:"le_m_1998"`
	LEM1999 string `csv:"le_m_1999"`
	LEM2000 string `csv:"le_m_2000"`
	LEM2001 string `csv:"le_m_2001"`
	LEM2002 string `csv:"le_m_2002"`
	LEM2003 string `csv:"le_m_2003"`
	LEM2004 string `csv:"le_m_2004"`
	LEM2005 string `csv:"le_m_2005"`
	LEM2006 string `csv:"le_m_2006"`
	LEM2007 string `csv:"le_m_2007"`
	LEM2008 string `csv:"le_m_2008"`
	LEM2009 string `csv:"le_m_2009"`
	LEM2010 string `csv:"le_m_2010"`
	LEM2011 string `csv:"le_m_2011"`
	LEM2012 string `csv:"le_m_2012"`
	LEM2013 string `csv:"le_m_2013"`
	LEM2014 string `csv:"le_m_2014"`
	LEM2015 string `csv:"le_m_2015"`
	LEM2016 string `csv:"le_m_2016"`
	LEM2017 string `csv:"le_m_2017"`
	LEM2018 string `csv:"le_m_2018"`
	LEM2019 string `csv:"le_m_2019"`
	LEM2020 string `csv:"le_m_2020"`
	LEM2021 string `csv:"le_m_2021"`
	LEM2022 string `csv:"le_m_2022"`
	LEM2023 string `csv:"le_m_2023"`

	// Expectativa anos na escola
	EYS1991 string `csv:"eys_1991"`
	EYS1992 string `csv:"eys_1992"`
	EYS1993 string `csv:"eys_1993"`
	EYS1994 string `csv:"eys_1994"`
	EYS1995 string `csv:"eys_1995"`
	EYS1996 string `csv:"eys_1996"`
	EYS1997 string `csv:"eys_1997"`
	EYS1998 string `csv:"eys_1998"`
	EYS1999 string `csv:"eys_1999"`
	EYS2000 string `csv:"eys_2000"`
	EYS2001 string `csv:"eys_2001"`
	EYS2002 string `csv:"eys_2002"`
	EYS2003 string `csv:"eys_2003"`
	EYS2004 string `csv:"eys_2004"`
	EYS2005 string `csv:"eys_2005"`
	EYS2006 string `csv:"eys_2006"`
	EYS2007 string `csv:"eys_2007"`
	EYS2008 string `csv:"eys_2008"`
	EYS2009 string `csv:"eys_2009"`
	EYS2010 string `csv:"eys_2010"`
	EYS2011 string `csv:"eys_2011"`
	EYS2012 string `csv:"eys_2012"`
	EYS2013 string `csv:"eys_2013"`
	EYS2014 string `csv:"eys_2014"`
	EYS2015 string `csv:"eys_2015"`
	EYS2016 string `csv:"eys_2016"`
	EYS2017 string `csv:"eys_2017"`
	EYS2018 string `csv:"eys_2018"`
	EYS2019 string `csv:"eys_2019"`
	EYS2020 string `csv:"eys_2020"`
	EYS2021 string `csv:"eys_2021"`
	EYS2022 string `csv:"eys_2022"`
	EYS2023 string `csv:"eys_2023"`

	// Expectativa anos na escola - Feminino
	EYF1991 string `csv:"eys_f_1991"`
	EYF1992 string `csv:"eys_f_1992"`
	EYF1993 string `csv:"eys_f_1993"`
	EYF1994 string `csv:"eys_f_1994"`
	EYF1995 string `csv:"eys_f_1995"`
	EYF1996 string `csv:"eys_f_1996"`
	EYF1997 string `csv:"eys_f_1997"`
	EYF1998 string `csv:"eys_f_1998"`
	EYF1999 string `csv:"eys_f_1999"`
	EYF2000 string `csv:"eys_f_2000"`
	EYF2001 string `csv:"eys_f_2001"`
	EYF2002 string `csv:"eys_f_2002"`
	EYF2003 string `csv:"eys_f_2003"`
	EYF2004 string `csv:"eys_f_2004"`
	EYF2005 string `csv:"eys_f_2005"`
	EYF2006 string `csv:"eys_f_2006"`
	EYF2007 string `csv:"eys_f_2007"`
	EYF2008 string `csv:"eys_f_2008"`
	EYF2009 string `csv:"eys_f_2009"`
	EYF2010 string `csv:"eys_f_2010"`
	EYF2011 string `csv:"eys_f_2011"`
	EYF2012 string `csv:"eys_f_2012"`
	EYF2013 string `csv:"eys_f_2013"`
	EYF2014 string `csv:"eys_f_2014"`
	EYF2015 string `csv:"eys_f_2015"`
	EYF2016 string `csv:"eys_f_2016"`
	EYF2017 string `csv:"eys_f_2017"`
	EYF2018 string `csv:"eys_f_2018"`
	EYF2019 string `csv:"eys_f_2019"`
	EYF2020 string `csv:"eys_f_2020"`
	EYF2021 string `csv:"eys_f_2021"`
	EYF2022 string `csv:"eys_f_2022"`
	EYF2023 string `csv:"eys_f_2023"`

	// Expectativa anos na escola - Masculino
	EYM1991 string `csv:"eys_m_1991"`
	EYM1992 string `csv:"eys_m_1992"`
	EYM1993 string `csv:"eys_m_1993"`
	EYM1994 string `csv:"eys_m_1994"`
	EYM1995 string `csv:"eys_m_1995"`
	EYM1996 string `csv:"eys_m_1996"`
	EYM1997 string `csv:"eys_m_1997"`
	EYM1998 string `csv:"eys_m_1998"`
	EYM1999 string `csv:"eys_m_1999"`
	EYM2000 string `csv:"eys_m_2000"`
	EYM2001 string `csv:"eys_m_2001"`
	EYM2002 string `csv:"eys_m_2002"`
	EYM2003 string `csv:"eys_m_2003"`
	EYM2004 string `csv:"eys_m_2004"`
	EYM2005 string `csv:"eys_m_2005"`
	EYM2006 string `csv:"eys_m_2006"`
	EYM2007 string `csv:"eys_m_2007"`
	EYM2008 string `csv:"eys_m_2008"`
	EYM2009 string `csv:"eys_m_2009"`
	EYM2010 string `csv:"eys_m_2010"`
	EYM2011 string `csv:"eys_m_2011"`
	EYM2012 string `csv:"eys_m_2012"`
	EYM2013 string `csv:"eys_m_2013"`
	EYM2014 string `csv:"eys_m_2014"`
	EYM2015 string `csv:"eys_m_2015"`
	EYM2016 string `csv:"eys_m_2016"`
	EYM2017 string `csv:"eys_m_2017"`
	EYM2018 string `csv:"eys_m_2018"`
	EYM2019 string `csv:"eys_m_2019"`
	EYM2020 string `csv:"eys_m_2020"`
	EYM2021 string `csv:"eys_m_2021"`
	EYM2022 string `csv:"eys_m_2022"`
	EYM2023 string `csv:"eys_m_2023"`

	// Média de anos na escola
	MYS1991 string `csv:"mys_1991"`
	MYS1992 string `csv:"mys_1992"`
	MYS1993 string `csv:"mys_1993"`
	MYS1994 string `csv:"mys_1994"`
	MYS1995 string `csv:"mys_1995"`
	MYS1996 string `csv:"mys_1996"`
	MYS1997 string `csv:"mys_1997"`
	MYS1998 string `csv:"mys_1998"`
	MYS1999 string `csv:"mys_1999"`
	MYS2000 string `csv:"mys_2000"`
	MYS2001 string `csv:"mys_2001"`
	MYS2002 string `csv:"mys_2002"`
	MYS2003 string `csv:"mys_2003"`
	MYS2004 string `csv:"mys_2004"`
	MYS2005 string `csv:"mys_2005"`
	MYS2006 string `csv:"mys_2006"`
	MYS2007 string `csv:"mys_2007"`
	MYS2008 string `csv:"mys_2008"`
	MYS2009 string `csv:"mys_2009"`
	MYS2010 string `csv:"mys_2010"`
	MYS2011 string `csv:"mys_2011"`
	MYS2012 string `csv:"mys_2012"`
	MYS2013 string `csv:"mys_2013"`
	MYS2014 string `csv:"mys_2014"`
	MYS2015 string `csv:"mys_2015"`
	MYS2016 string `csv:"mys_2016"`
	MYS2017 string `csv:"mys_2017"`
	MYS2018 string `csv:"mys_2018"`
	MYS2019 string `csv:"mys_2019"`
	MYS2020 string `csv:"mys_2020"`
	MYS2021 string `csv:"mys_2021"`
	MYS2022 string `csv:"mys_2022"`
	MYS2023 string `csv:"mys_2023"`

	// Média de anos na escola - Feminino
	MYF1991 string `csv:"mys_f_1991"`
	MYF1992 string `csv:"mys_f_1992"`
	MYF1993 string `csv:"mys_f_1993"`
	MYF1994 string `csv:"mys_f_1994"`
	MYF1995 string `csv:"mys_f_1995"`
	MYF1996 string `csv:"mys_f_1996"`
	MYF1997 string `csv:"mys_f_1997"`
	MYF1998 string `csv:"mys_f_1998"`
	MYF1999 string `csv:"mys_f_1999"`
	MYF2000 string `csv:"mys_f_2000"`
	MYF2001 string `csv:"mys_f_2001"`
	MYF2002 string `csv:"mys_f_2002"`
	MYF2003 string `csv:"mys_f_2003"`
	MYF2004 string `csv:"mys_f_2004"`
	MYF2005 string `csv:"mys_f_2005"`
	MYF2006 string `csv:"mys_f_2006"`
	MYF2007 string `csv:"mys_f_2007"`
	MYF2008 string `csv:"mys_f_2008"`
	MYF2009 string `csv:"mys_f_2009"`
	MYF2010 string `csv:"mys_f_2010"`
	MYF2011 string `csv:"mys_f_2011"`
	MYF2012 string `csv:"mys_f_2012"`
	MYF2013 string `csv:"mys_f_2013"`
	MYF2014 string `csv:"mys_f_2014"`
	MYF2015 string `csv:"mys_f_2015"`
	MYF2016 string `csv:"mys_f_2016"`
	MYF2017 string `csv:"mys_f_2017"`
	MYF2018 string `csv:"mys_f_2018"`
	MYF2019 string `csv:"mys_f_2019"`
	MYF2020 string `csv:"mys_f_2020"`
	MYF2021 string `csv:"mys_f_2021"`
	MYF2022 string `csv:"mys_f_2022"`
	MYF2023 string `csv:"mys_f_2023"`

	// Média de anos na escola - Masculino
	MYM1991 string `csv:"mys_m_1991"`
	MYM1992 string `csv:"mys_m_1992"`
	MYM1993 string `csv:"mys_m_1993"`
	MYM1994 string `csv:"mys_m_1994"`
	MYM1995 string `csv:"mys_m_1995"`
	MYM1996 string `csv:"mys_m_1996"`
	MYM1997 string `csv:"mys_m_1997"`
	MYM1998 string `csv:"mys_m_1998"`
	MYM1999 string `csv:"mys_m_1999"`
	MYM2000 string `csv:"mys_m_2000"`
	MYM2001 string `csv:"mys_m_2001"`
	MYM2002 string `csv:"mys_m_2002"`
	MYM2003 string `csv:"mys_m_2003"`
	MYM2004 string `csv:"mys_m_2004"`
	MYM2005 string `csv:"mys_m_2005"`
	MYM2006 string `csv:"mys_m_2006"`
	MYM2007 string `csv:"mys_m_2007"`
	MYM2008 string `csv:"mys_m_2008"`
	MYM2009 string `csv:"mys_m_2009"`
	MYM2010 string `csv:"mys_m_2010"`
	MYM2011 string `csv:"mys_m_2011"`
	MYM2012 string `csv:"mys_m_2012"`
	MYM2013 string `csv:"mys_m_2013"`
	MYM2014 string `csv:"mys_m_2014"`
	MYM2015 string `csv:"mys_m_2015"`
	MYM2016 string `csv:"mys_m_2016"`
	MYM2017 string `csv:"mys_m_2017"`
	MYM2018 string `csv:"mys_m_2018"`
	MYM2019 string `csv:"mys_m_2019"`
	MYM2020 string `csv:"mys_m_2020"`
	MYM2021 string `csv:"mys_m_2021"`
	MYM2022 string `csv:"mys_m_2022"`
	MYM2023 string `csv:"mys_m_2023"`
}

func Runner() {
	runnerName := "IDH"

	fullURLFile := "https://hdr.undp.org/modules/custom/hdro_app/static/media/Onlinemaster_HDR2122_081522.ac8500f84b9d9d251f41.csv"
	fileNameRaw := "./data/idh/raw/hdr-raw.csv"
	fileNameOutput := "./data/idh/idh.json"
	fileNameOutputCSV := "./data/idh/idh.csv"
	fonte := "https://hdr.undp.org/"

	l := logger.Instance()
	now := time.Now()
	hdi := &HDI{}
	hdi.Fonte = fonte
	hdi.Atualizacao = now

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Criando o arquivo de recepção temporario")

	f, err := os.Create(fileNameRaw)
	defer f.Close()

	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Erro", err.Error()).
			Msg("Falha ao criar o arquivo temporario")
	}

	client := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Str("URL", fullURLFile).
		Msg("Realizando o download do arquivo")

	resp, err := client.Get(fullURLFile)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao fazer o request HTTP para a URL selecionada")
	}
	defer resp.Body.Close()

	size, err := io.Copy(f, resp.Body)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao escrever no arquivo temporario")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Str("URL", fullURLFile).
		Int64("Size", size).
		Msg("Escrita no arquivo temporário concluído")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Lendo o arquivo temporário")

	tmpFile, err := os.OpenFile(fileNameRaw, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer tmpFile.Close()

	hdiCsv := []*HDICsv{}

	if err := gocsv.UnmarshalFile(tmpFile, &hdiCsv); err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao converter o CSV em Struct")
		panic(err)
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Recuperando os dados brasileiros")

	ordenado := make(map[string]Data)

	for _, pais := range hdiCsv {

		if pais.Pais == "Brazil" {

			// Recupera o nome dos campos da Struct
			campos := structs.Names(pais)

			// Construindo o Campo do IDH Geral
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "HDI") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := &Data{
						Ano: ano,
						IDH: valor,
					}

					if item.IDH > 0 {
						ordenado[ano] = *item
					}

				}

			}

			// Construindo o Campo do IDH Feminino
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "FHD") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.IDHF = valor

					ordenado[ano] = item

				}

			}

			// Construindo o Campo do IDH Masculino
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "MHD") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.IDHM = valor

					ordenado[ano] = item

				}

			}

			// Construindo a expectativa de vida - Geral
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "LEG") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeVida = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

			}

			// Construindo a expectativa de vida - Feminina
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "LEF") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeVidaFeminina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

			}

			// Construindo a expectativa de vida - Masculina
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "LEM") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeVidaMasculina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

			}

			// Construindo a expectativa de anos na escola
			for _, v := range campos[1:] {

				// Expectativa Anos na Escola - Geral
				if strings.HasPrefix(v, "EYS") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeAnosNaEscola = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

				// Expectativa Anos na Escola - Feminino
				if strings.HasPrefix(v, "EYF") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeAnosNaEscolaFeminina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

				// Expectativa Anos na Escola - Masculino
				if strings.HasPrefix(v, "EYM") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.ExpectativaDeAnosNaEscolaMasculina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

			}

			// Construindo a média de anos na escola
			for _, v := range campos[1:] {

				// Média de Anos na Escola - Geral
				if strings.HasPrefix(v, "MYS") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.MediaDeAnosNaEscola = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

				// Média de Anos na Escola - Feminino
				if strings.HasPrefix(v, "MYF") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.MediaDeAnosNaEscolaFeminina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

				// Média de Anos na Escola - Masculino
				if strings.HasPrefix(v, "MYM") {

					r := reflect.ValueOf(pais)
					f := reflect.Indirect(r).FieldByName(v)

					ano := v[3:7]

					valorStr := fmt.Sprintf("0%v", f.String())
					valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

					if err != nil {
						l.Fatal().
							Str("Runner", runnerName).
							Str("Error", err.Error()).
							Str("Valor recuperado", valorStr).
							Msg("Erro ao converter o valor para Float64")
					}

					item := ordenado[ano]
					item.MediaDeAnosNaEscolaMasculina = math.Round(valor*100) / 100

					ordenado[ano] = item

				}

			}

		}

	}

	for _, i := range ordenado {

		if i.Ano != "" {
			hdi.Data = append(hdi.Data, i)
		}

	}

	sort.Slice(hdi.Data, func(i, j int) bool {
		return hdi.Data[i].Ano < hdi.Data[j].Ano
	})

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Criando o arquivo final")

	o, err := os.Create(fileNameOutput)
	defer o.Close()

	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("Erro", err.Error()).
			Msg("Falha ao criar o arquivo final")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Arquivo criado")

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(hdi)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = o.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Removendo arquivo temporario")

	err = os.Remove(fileNameRaw)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
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

	csvOutput, err := gocsv.MarshalString(&hdi.Data)
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
			Str("FilePath", fileNameOutput).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Finalizado")

}
