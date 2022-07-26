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
             vol_riego_p_ha = 0,
             simular = 0
             ):

       self.padron = pd.read_json(padron).fillna(value=0)  # Objeto json del padrón de riego.
       self.refuerzo = pd.read_json(refuerzo).fillna(value=0) # Objeto json del los refuerzos vincualdos al padrón.
       self.solicitud = pd.read_json(solicitud).fillna(value=0) # Objeto json de las solicitudes de riego vinculadas al padrón.
       self.reservorio = pd.read_json(reservorio).fillna(value=0) # Objeto json de los reservorios vinculados al padrón.
       self.caudal_canal = caudal_canal
       self.dur_turno = dur_turno
       self.fecha_inicio = pd.to_datetime(fecha_inicio, dayfirst = True, errors = 'ignore')
       self.modos = pd.read_json(modos) #Objeto JSON con los modos de riego y distribución (cabeza_cola)
       self.vol_riego_p_ha = vol_riego_p_ha #dato que se pasa al generar el turno.
       self.simular = simular

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

        for cauce, dato in self.cauces_g:
            if self.modos.cabeza_cola[cauce] == 0: #Riego por COLA
                self.cauces['tpo_recorrido'] = self.cauces['tpo_rec_toma'] + self.cauces['tpo_rec_cabeza_cola']
            else: #Riego por CABEZA
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

    def set_modo_riego(self):
        '''
        Método que analiza los modos de riego de grupo y subgrupo para asignar el tiempo de turno y fecha de inicio
        a cada uno de las cauces del padrón.
        :return: DF integrado por una serie con los tiempos de turnado en formado timedelta y una serie con las fechas
        de inicio en formato datetime.
        '''
        stack = [0]
        turnado_c = pd.Series([0])
        turnado_sg = pd.Series([0])
        inicio_c = pd.Series([self.fecha_inicio])
        inicio_sg = pd.Series([self.fecha_inicio])

        for cauce,subgrupo in self.subgrupos_cauce.index:
            # Factor que contempla la relación de Vsg/Vg
            f_g = self.get_vol_riego().subgrupo[subgrupo] / self.get_vol_riego().cauce.sum()
            # Factor que contempla la relación de Vc/Vsg
            f_sg = self.get_vol_riego().cauce[cauce] / self.get_vol_riego().subgrupo[subgrupo]

            # Análisis de modos de riego para asignar tiempo de turnado y fecha de inicio al turnado de cada cauce
            # Caso 0: GRUPO Y SUBGRUPO SECUENCIAL
            if (self.modos.grupo[cauce] == 1) and (self.modos.subgrupo[cauce] == 1):
                turnado_c[cauce] = f_g * f_sg * self.dur_turno
                inicio_c[cauce] = inicio_c[cauce-1] \
                                  + pd.to_timedelta(turnado_c[cauce-1], unit='d')

            # Caso 1: GRUPO SECUENCIAL Y SUBGRUPO INDEPENDIENTE
            elif (self.modos.grupo[cauce] == 1) and (self.modos.subgrupo[cauce] == 0):
                turnado_c[cauce] = f_g * self.dur_turno
                turnado_sg[subgrupo] = f_g * self.dur_turno
                inicio_sg[subgrupo] = inicio_sg[subgrupo-1] \
                                      + pd.to_timedelta(turnado_sg[subgrupo-1], unit='d')
                inicio_c[cauce] = inicio_sg[subgrupo]

            # Caso 2: GRUPO INDEPENDIENTE Y SUBGRUPO SECUENCIAL - REVISAR!
            elif (self.modos.grupo[cauce] == 0) and (self.modos.subgrupo[cauce] == 1):
                stack.append(cauce + subgrupo) #Artilugio algebraico para identificar el cambio de SG
                bandera = stack[cauce] - stack[cauce - 1] #Evita secuenciar los turnados al cambiar el SG

                turnado_c[cauce] = f_sg * self.dur_turno
                turnado_sg[subgrupo] = self.dur_turno
                inicio_sg[subgrupo] = self.fecha_inicio
                inicio_c[cauce]=inicio_sg[subgrupo] \
                                + pd.to_timedelta(turnado_c[cauce-1], unit='d') \
                                * int((bandera == 1))

            # Caso 3: GRUPO Y SUBGRUPO INDEPENDIENTE. Parametrización por defecto.
            else:
                turnado_c[cauce] = self.dur_turno
                inicio_c[cauce] = self.fecha_inicio

        modo_riego_df = pd.DataFrame({'turnado_c':turnado_c,
                                      'turnado_sg': turnado_sg,
                                      'inicio_sg':inicio_sg,
                                      'inicio_c':inicio_c}
                                    )
        return modo_riego_df

    def get_caudal_riego(self): #Contemplar recibir el padron del escenario de simulación
        #self.simular = simular
        '''
        Genera el caudal de riego por cauce dentro del padrón asignado para turnado.
        :return: DF con los caudales agregados por niveles grupo/subgrupo/cauce
        '''
        caudal_riego = pd.Series([0],dtype=float)
        if self.simular==1: #Desde el Simulador toma el vol_riego_p_ha para determinar los caudales.
            for cauce, dato in self.cauces_g:
                caudal_riego[cauce] = ((self.vol_riego_p_ha * self.get_sup_riego().cauce[cauce]) / self.set_modo_riego().turnado[cauce]) * (1 / self.f_escala)
        else:
            for cauce, dato in self.cauces_g:
                caudal_riego[cauce] = (self.get_vol_riego().cauce[cauce] / self.set_modo_riego().turnado[cauce]) * (1 / self.f_escala)

        return caudal_riego

    def get_subpadron(self):
        '''
        Segmenta los padrones de cada cauce para ordenar en base a la estrategia de riego (cabeza_cola).
        :return: Diccionario con los padrones por cauce y ordenados.
        '''
        self.subpadron = {}

        for cauce,dato in self.cauces_g:
           # segmentación de padron y solicitudes en cauces. OK
           self.subpadron[cauce] = self.cauces_g.get_group(cauce)

           # ordenamiento de cada cauce por cabeza_cola. OK
           self.subpadron[cauce] = self.subpadron[cauce].sort_index(ascending=self.cabeza_cola_bool[cauce])

        return self.subpadron

class cuadroTurno:

    f_lamina = 10

    def __init__(self, padron, tiempo, inicio, caudal, volumen, vol_riego_p_ha, volumen_tiempo = 1):
        #Parámetros que se pasan desde red = redSecundaria().
        self.padron = padron.fillna(value=0) #red.get_subpadron()[cauce]
        self.inicio = inicio #red.set_modo_riego().inicio[cauce]
        self.caudal = caudal #red.get_caudal_riego(simular)[cauce] puede ser simulado o no.
        self.volumen = volumen #red.get_subpadron()[cauce].sup_riego * red.get_vol_riego_ha()
        self.tiempo = tiempo #red.get_subpadron()[cauce].sup_riego * red.get_tpo_riego_ha(). REVISAR: falta contemplar el tpo_red

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
        df_turno = pd.DataFrame({'CC': self.padron['CC'],
                                 'PP': self.padron['PP'],
                                 'Caudal': self.caudal,
                                 'Volumen': self.volumen,
                                 'Inicio': pd.to_datetime('01-01-2020 00:00'),
                                 'Tiempo': pd.to_timedelta(self.tiempo, unit = 'd'),
                                 'Fin': pd.to_datetime('01-01-2020 00:00'),
                                 'id_parcela': self.padron['idPadron']
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

        #Convierto a string los datetime para la presentación en las vistas JS
        formato_la ='Fecha:%d-%m-%Y Hora:%H:%M'
        n = 0
        while n in range(len(self.padron)):
            df_turno.loc[n,'Fin'] = df_turno.loc[n,'Fin'].strftime(formato_la)
            df_turno.loc[n,'Inicio'] = df_turno.loc[n,'Inicio'].strftime(formato_la)
            df_turno.loc[n, 'Tiempo'] = str(df_turno.loc[n, 'Tiempo']) #Esta conversión no es necesaria para el caso de JS
            n = n + 1

        df_turno['Caudal'] = self.caudal

        #Presenta volumen y tiempo o sólo tiempo de riego en el cuadro de turnos
        if self.volumen_tiempo == 0:
            df_turno['Volumen'] = 0

        #Cambia el índice al idPdron y presenta en json con clave en base al índice
        df_turno.index = self.padron['idPadron']
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
        df_cuenta_agua = pd.DataFrame({'CC': self.padron['CC'],
                                       'PP': self.padron['PP'],
                                       'Agua Programada':lamina_p,
                                       'Agua Entregada': lamina_e,
                                       'Balance': cuenta_agua,
                                       'Volumen Entregado': self.volumen,
                                       'id_parcela':self.padron['idPadron']}).reset_index(drop=True)

        df_cuenta_agua.index = self.padron['idPadron']
        return df_cuenta_agua.to_json(orient = 'index', double_precision = 1)
