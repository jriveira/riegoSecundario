#Importamos los paquetes complementarios:
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy


class redSecundaria:
    """
    Clase Riego Secundario:
    Tiene como objeto principal generar el cuadro de turno para distribución secundaria de una red de riego,
    presentar la cuenta de agua y calcular los caudales para estabilizar el canal.
    -Implementa los procedimientos de cálculo para un esquema de distribución secundaria de riego acordado
    -Calcula volumen y lámina para la cuenta de agua de cada parcela.
    -Calcula los caudales para estabilizar el canal (Simulador)
    """

    #    Definición de propiedades de la clase
    f_tiempo = 24*60 #min/dia
    f_escala = ((24*60*60)/1000)
    f_lamina = 10

    #Parámetros de configuración de la Inspección
    f_compensa = 1 #factor de compensación x pérdidas del canal

    #revisar que se calcule a partir de una valor de volumen de la Inspección sobre sup a distribuir
    # vol_riego_p_ha = vol_riego_p / sum(self.padron['sup_emp_reducida'])

#   Constructor de la clase / Propiedades
    '''
    Listado del padrón de regantes: padron
    Duración del turno: dur_turno
    Caudal en cabecera de canal: caudal_canal
    Fecha de inicio de la programación del turno: fecha_inicio
    Estrategia de riego: cabeza_cola
    Tiempo de recorrido de toma: tpo_rec_toma
    Tiempo de recorrido de cauce para riego cabeza - cola: tpo_rec_cabeza_cola
    Tiempo de recorrido de cauce para riego cola - cabeza: tpo_rec_cola_cabeza
    Tiempo de descuelgue: tpo_descuelgue
    Superficie empadronada reducida: sup_emp_reducida
    Superficie adicional: sup_ad
    Superficie restringida: sup_res
    Superficie recibida: sup_rec
    Superficie cedida: sup_cedida
    Hectárea activa: ha_activa
    Hectátrea sí/no :ha_si
    Caudal de refuerzo: caudal_refuerzo
    Duración de refuerzo: dur_refuerzo
    Intensidad de refierzo: int_refuerzo
    Factor de compensación_ f_compensa
    Volumen de riego programado: vol_riego_p
    '''

    def __init__(self,
             padron,
             refuerzo = 0,
             solicitud = 0,
             reservorio = 0,
             caudal_canal = 100,
             dur_turno = 24,
             fecha_inicio = "01-01-2022",
             modos = 0,
             vol_riego_p_ha = 0
             ):

       self.padron = pd.read_json(padron)  # Objeto json del padrón de riego.
       self.refuerzo = pd.read_json(refuerzo) # Objeto json del los refuerzos vincualdos al padrón.
       self.solicitud = pd.read_json(solicitud) # Objeto json de las solicitudes de riego vinculadas al padrón.
       self.reservorio = pd.read_json(reservorio) # Objeto json de los reservorios vinculados al padrón.
       self.caudal_canal = caudal_canal
       self.dur_turno = dur_turno
       self.fecha_inicio = pd.to_datetime(fecha_inicio, dayfirst = True, errors = 'ignore')
       self.modos = pd.read_json(modos) #Arreglo de modos de riego Grupos/Subgrupos/Cauces
       self.vol_riego_p_ha = vol_riego_p_ha #dato que se pasa al generar el turno.

       # 1-Integra las solicitudes de riego al padron y calcula la superficie efecutiva de riego
       self.padron['sup_anexa'] = self.solicitud['sup_ad'] - self.solicitud['sup_res']
       self.padron['sup_pase'] = self.solicitud['sup_rec'] - self.solicitud['sup_ced']
       self.padron['sup_riego'] = (self.padron['sup_emp_reducida'] + self.padron['sup_anexa'] + self.padron['sup_pase']) \
                                  * self.padron["ha_si"] * self.solicitud["ha_activa"]

       # 2-Agrupa y agrega padron por cauces / subgrupos / grupos
       self.cauces_g = self.padron.groupby('orden_cauce')
       self.cauces = self.padron.groupby('orden_cauce').sum()
       self.subgrupos = self.padron.groupby('Subgrupo').sum()
       self.grupos = self.padron.groupby('Grupo').sum()
       self.subgrupos_cauce = self.padron.groupby(['orden_cauce','Subgrupo']).sum()


       self.ctd_cauces = len(self.cauces)
       self.ctd_subgrupos = len(self.subgrupos)
       self.ctd_grupos = len(self.grupos)

       # 3-Ordena cada cauce en base a los modos de distribución (cabeza_cola)
       # Toma como referencia el riego por cabeza:'ascending=True' para el .sort_index()
       self.cabeza_cola_bool = self.modos.cabeza_cola == 1

    # Métodos generales de la clase: determinación de superficie, volumen, tiempo y caudal.

    def get_sup_riego(self):
        """
        Este método devuelve la superficie de riego que se requiere en el cálculo de turno o para simulación de caudales.
        Es aplicable en los distintos ámbitos: parcela/toma/cauce(hijuela)/canal(inspección)

        Datos obtenidos del objeto padron:
        1)Superficie empadronada reducida: sup_emp_reducida
        Dato obtenido del servicio (WS) de padrón de DGI.
        Dato principal afectado al cálculo de la superficie de riego.
        2)Superficie adicional:sup_ad
        Superficie adicional que solicita el regante mediante la solicitud de riego.
        3)Superficie restringida: sup_res
        Superficie que compensa el pedio de superficie adicional.
        Lo determina el regante en la solicitud de riego para el balance de riego cuando solicita una sup_ad.
        4)Superficie recibida:sup_rec
        Superficie recibida por pases que son establecidos mediante solicitudes de riego de los regantes.
        5)Superficie cedida:sup_cedida
        Superficie cedida por pases que son establecidos mediante solicitudes de riego de los regantes.
        6)Has activas:ha_activa
        Factor que ajusta la superficie activa para riego.
        7)Condición de ha si/no:ha_si
        Estado de habilitación del padrón obtenido del servicio (SW) de Ha si/no del DGI.

        Resultado:
        Superficie de riego:sup_riego
        Valor obtenido para determinar la superficie efectiva para riego de un padrón, cauce o canal contemplando las
        solicitudes de los regantes.
        En caso de simulación (simular = 1) se devuelve el agregado de las superficies por Grupo/Subgrupo/Cauces-
        """
        sup_riego_df = pd.DataFrame({'cauce': self.cauces.sup_riego,
                                     'subgrupo': self.subgrupos.sup_riego,
                                     'grupo': self.grupos.sup_riego}).fillna(value=0)
        return sup_riego_df # Devuelve un DF con las sup_riego agregadas por niveles

    def get_tpo_red(self):
        '''
        Este método devuelve un vector con el tiempo de red que corresónde a cada cauce del padrón.
        Dependerá de la estrategia de riego cabeza-cola de cada cauce.
        Para determinar el tpo_riego_ha se requiere tomar la suma de todos los valores
        '''

        for cauce in range(1, (self.ctd_cauces + 1)):
            if self.modos.cabeza_cola[cauce] == 0:
                self.cauces['tpo_recorrido'] = self.cauces['tpo_rec_toma'] + self.cauces['tpo_rec_cabeza_cola']
            else:
                self.cauces['tpo_recorrido'] = self.cauces['tpo_rec_cola_cabeza']

        self.cauces['tpo_red'] = self.cauces['tpo_descuelgue'] - self.cauces['tpo_recorrido']

        return self.cauces['tpo_red'] # Devuelve una serie con el tpo_red por cauce

    def get_cap_refuerzo(self):
        """
        Este método devuelve el refuerzo disponible en la inspección que se confirgura con los pozos u otras fuentes de refuerzo.
        considerar que tanto caudal_refuerzo como dur_refuerzo son vectores con los datos obtenidos de la configuración de pozos.

        Datos:
        1)Caudal de refuerzo:caudal_refuerzo
        Dato de caudal parametrizado por el AdT al momento de configurar los pozos en la inspección. Es una lista con los caudales
        de los pozos.
        2)Duración de refuerzo: dur_refuerzo
        Dato de duración que complementa al caudal,parametrizado por el AdT al momento de configurar los pozos en la inspección.
        Es una lista con las duraciones de refuerzo.
        Resultado:
        Capacidad de refuerzo: cap_refuerzo.
        Valor obtenido de las sumas de todos los aportes realizados por los pozos de la inspección que determina el total
        de refuerzo disponible mediante este mecanismo de aporte.
        """
        cap_refuerzo = sum(self.refuerzo['caudal_refuerzo'])*sum(self.refuerzo['dur_refuerzo'])*self.f_escala
        return cap_refuerzo

    def get_reservorio(self):
        """
        Consolida el volumen de los reservorios vinculados al padrón
        """
        reservorio = sum(self.reservorio["volumen"])
        return reservorio

    def get_vol_riego_ha(self):
        """
        Este método devuelve el volumen de riego por ha que se debe aplicar para la determinación del volumen de riego
        efectivo en todo el padron. Contempla descontar el tiempo de red, que son los tiempos de la inercia propia
        de la red de distribución
        También se contempla por medio del parámetro f_compensa las pérdidas del canal.
        La determinación del volumen de riego se plantea en todos los ámbitos:parcela/toma/cauce(hijuela)/canal (inspección)
        El vol_riego será el determinado por el ámbito donde se aplique el método.
        """
        cr = self.caudal_canal / self.get_sup_riego().cauce.sum()
        vol_base_ha = cr * (self.dur_turno-(self.get_tpo_red().sum()/self.f_tiempo)) * self.f_escala
        refuerzo = self.get_cap_refuerzo() #* self.int_refuerzo
        reservorio = self.get_reservorio()
        vol_riego_ha = vol_base_ha * self.f_compensa + ((refuerzo + reservorio) / self.get_sup_riego().cauce.sum())
        return vol_riego_ha

    def get_vol_riego(self):
        '''
        Esté método devuelve un DF con el volumen de riego agregado por cada nivel.
        :return: Diccionario con los volúmenes de riego por cauce organizados por orden de riego en el padron.
        '''
        vol_riego_df = self.get_sup_riego() * self.get_vol_riego_ha()
        return vol_riego_df

    def get_tpo_riego_ha(self):
        tpo_riego_ha = (self.dur_turno-(self.get_tpo_red().sum()/self.f_tiempo)) / self.get_sup_riego().cauce.sum()
        return tpo_riego_ha

    def get_tpo_turnado(self):
        '''
        Define el tpo_turnado agregado por cada nivel que será asignado como dur_turno a cada cauce
        en base a los modos de riego definidos para cada cauce.
        Se expresa en timedelta.
        '''

        vol_turno = self.get_vol_riego().cauce.sum()
        f_turnado = (self.dur_turno / vol_turno)

        '''
        tpo_turnado_cauce = pd.to_timedelta((self.get_vol_riego().cauce * f_turnado),unit = 'd')
        tpo_turnado_subgrupo = pd.to_timedelta((self.get_vol_riego().subgrupo * f_turnado),unit = 'd')
        tpo_turnado_grupo = pd.to_timedelta((self.get_vol_riego().grupo * f_turnado),unit = 'd')
        tpo_turnado_df = pd.DataFrame({'cauce':tpo_turnado_cauce,
                                       'subgrupo': tpo_turnado_subgrupo,
                                       'grupo': tpo_turnado_grupo}
                                      )

        '''
        tpo_turnado_df = (self.get_vol_riego() * f_turnado)
        return tpo_turnado_df

    def get_fecha_inicio_turnado(self):
        '''
        Determina las fechas de inicio de cauces y subgrupos a partir del DF de tpo_turnado.
        Estas fechas se aplicarán a la ivocación de turnos en base a los modos de riego en set_modo_riego().
        Se expresa como tipo datetime.
        '''

        fecha_fin_c = self.fecha_inicio + pd.to_timedelta(self.get_tpo_turnado().cauce, unit='d')
        fecha_fin_sg = self.fecha_inicio + pd.to_timedelta(self.get_tpo_turnado().subgrupo, unit='d')
        fecha_fin_df = pd.DataFrame({'cauce':fecha_fin_c,
                                     'subgrupo':fecha_fin_sg})

        return fecha_fin_df

    def set_modo_riego(self):
        '''
        Método que analiza los modos de riego de grupo y subgrupo para asignar la duración de turno y fecha de inicio
        a cada uno de las cauces del padron.
        :return: DF integrado por una serie con los tiempos de turnado en formado timedelta y una serie con las fechas
        de inicio en formato datetime.
        '''
        turnado = pd.Series([0])
        inicio = pd.Series([0])

        for cauce in range(1, (self.ctd_cauces + 1)):
            # Análisis de modos de riego para asignar tiempo de turnado y fecha de inicio al turnado de cada cauce
            if self.modos.subgrupo[cauce] == 1:  # SUBGRUPO SECUENCIAL, independientemente del modo de GRUPO
                turnado[cauce] = self.get_tpo_turnado().cauce[cauce]
                inicio[cauce] = self.get_fecha_inicio_turnado().cauce[cauce]  # fecha_fin cauce previo
                #inicio[cauce] = self.fecha_inicio + pd.to_timedelta(self.get_tpo_turnado().cauce[cauce-1], unit='d')

            elif (self.modos.subgrupo[cauce] == 0) and (self.modos.grupo[cauce] == 1):  # GRUPO SECUENCIAL Y SUBGRUPO INDEPENDIENTE
                subgrupo = sum(self.subgrupos_cauce.index[cauce-1]) - cauce # Artilugio algebraico para recuperar el id del subgrupo vinculado al cauce.
                turnado[cauce] = self.get_tpo_turnado().subgrupo[subgrupo]
                inicio[cauce] = self.get_fecha_inicio_turnado().subgrupo[subgrupo]  # REVISAR!!
                #inicio[cauce] = self.fecha_inicio + pd.to_timedelta(self.get_tpo_turnado().subgrupo[subgrupo], unit='d')

            else:  # GRUPO Y SUBGRUPO INDEPENDIENTE
                turnado[cauce] = self.dur_turno
                inicio[cauce] = self.fecha_inicio

        modo_riego_df = pd.DataFrame({'turnado':turnado,
                                      'inicio':inicio}
                                    )

        return modo_riego_df

    def get_caudal_riego(self,simular=0): #Contemplar recibir el padron del escenario de simulación
        self.simular = simular
        '''
        Genera el caudal de riego por cauce dentro del padrón asignado para turnado.
        :return: DF con los caudales agregados por niveles grupo/subgrupo/cauce
        '''
        if self.simular==1:
            caudal_riego = (self.vol_riego_p_ha * self.get_sup_riego().cauce / self.set_modo_riego().turnado) * (1 / self.f_escala)
        else:
            caudal_riego = (self.get_vol_riego().cauce / self.set_modo_riego().turnado) * (1 / self.f_escala)

        return caudal_riego

    def get_subpadron(self):
        '''
        Segmenta los padrones de cada cauce para ordenar en base a la estrategia de riego (cabeza_cola).
        :return: Diccionario con los padrones por cauce y ordenados.
        '''
        self.subpadron = {}

        for cauce in range(1, (self.ctd_cauces + 1)):
           # segmentación de padron y solicitudes en cauces. OK
           self.subpadron[cauce] = self.cauces_g.get_group(cauce)

           # ordenamiento de cada cauce por cabeza_cola. OK
           self.subpadron[cauce] = self.subpadron[cauce].sort_index(ascending=self.cabeza_cola_bool[cauce])

        return self.subpadron

class cuadroTurno:

    f_lamina = 10

    def __init__(self, padron, tiempo, inicio, caudal, volumen, vol_riego_p_ha, volumen_tiempo = 1):
        #Parámetros que se pasan desde redSecundaria().
        self.padron = padron.fillna(value=0) #get_subpadron()
        self.inicio = inicio #get_fecha_inicio()
        self.caudal = caudal #get_caudal_riego() puede ser simulado o no.
        self.volumen = volumen #get_vol_riego()
        self.tiempo = tiempo

        #Parámetros propios de la clase.
        self.vol_riego_p_ha = vol_riego_p_ha #dato que se pasa al generar el turno.
        self.volumen_tiempo = volumen_tiempo #dato que se pasa desde la configuración de la Inspección.

    def set_turno_riego(self):
        '''
        Este método devuelve el dataframe de turnado de toda una inspección, grupo , subgrupo, cauce o toma
        según el alcance de la superficie de riego asignada.
        La tabla se compone de los campos: Caudal, Volumen, Tiempo, Inicio y Fin.
        Este alcance se pasará con la variable lista que se compone de
        la secuancia de superficies organizadas por orden de riego.
        '''
        df_turno = pd.DataFrame({'Caudal': self.caudal,
                                 'Volumen': self.volumen,
                                 'Inicio': pd.to_datetime('01-01-2020 00:00'),
                                 'Tiempo': pd.to_timedelta(self.tiempo, unit = 'd'),
                                 'Fin': pd.to_datetime('01-01-2020 00:00')
                                 }).reset_index(drop=True) #reinicia el indice del DF para unificar el criterio de asignación en cada subpadron

        # el tiempo de riego asignado a la parcela se interpreta como timeoffset para la programación de inicio y fin de turno
        df_turno.loc[0,'Inicio'] = self.inicio
        df_turno.loc[0,'Fin'] = self.inicio + df_turno.loc[0,'Tiempo']

        # Desarrollo de vector de fechas de inicio y fin a partir de los tiempos de riego aprovechando la operatorias con datos datetime
        n = 1
        while n in range(len(self.padron)):
            df_turno.loc[n,'Fin'] = df_turno.loc[(n - 1),'Fin'] + df_turno.loc[n,'Tiempo']
            df_turno.loc[n,'Inicio'] = df_turno.loc[(n - 1),'Fin']
            n = n + 1

        #Convierto a string los datetime para la presentación en las vistas js
        formato_la ='Fecha:%d-%m-%Y Hora:%H:%M'
        n = 0
        while n in range(len(self.padron)):
            df_turno.loc[n,'Fin'] = df_turno.loc[n,'Fin'].strftime(formato_la)
            df_turno.loc[n,'Inicio'] = df_turno.loc[n,'Inicio'].strftime(formato_la)
            df_turno.loc[n, 'Tiempo'] = str(df_turno.loc[n, 'Tiempo'])
            n = n + 1

        df_turno['Caudal'] = self.caudal

        #Presenta volumen y tiempo o sólo tiempo de riego en el cuadro de turnos
        if self.volumen_tiempo == 0:
            df_turno['Volumen'] = 0

        #Compone datos complementarios en el Data Frame antes de generar el json
        df_turno['CC'] = self.padron['CC']
        df_turno['PP'] = self.padron['PP']
        df_turno['id_parcela'] = self.padron['id_parcela']

        #Cambia el índice al idPdron y presenta en json con clave en base al índice
        df_turno.index = self.padron['id_parcela']
        return df_turno.to_json(orient = 'index', date_format = 'iso', date_unit="s", double_precision = 1)

    def set_cuenta_agua(self):
        '''
                Este método calcula la cuenta de agua en los distintos ámbitos de alcance.
                Se expresa en láminas (mm).
                El volumen programado x ha (vol_riego_p_ha) es suministrado por la inspección como dato.
        '''
        lamina_p = (self.vol_riego_p_ha * self.padron['sup_riego']) / self.f_lamina # lámina programada por parcela
        lamina_e = self.volumen / self.f_lamina # lámina entregado por parcela en el turno

        cuenta_agua = lamina_p - lamina_e
        df_cuenta_agua = pd.DataFrame({'Agua Programada':lamina_p,
                                       'Agua Entregada': lamina_e,
                                       'Balance': cuenta_agua,
                                       'Volumen Entregado': self.volumen})

        #Compone datos complementarios en el Data Frame antes de generar el json
        df_cuenta_agua['CC'] = self.padron['CC']
        df_cuenta_agua['PP'] = self.padron['PP']
        df_cuenta_agua['id_parcela'] = self.padron['id_parcela']

        df_cuenta_agua.index = self.padron['id_parcela']
        return df_cuenta_agua.to_json(orient = 'index', double_precision = 1)
