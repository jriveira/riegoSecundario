import pandas as pd
from flask import Flask
from flask_cors  import CORS
from flask_restful import Api, Resource, reqparse
from Clase_dis_sec_v3_1 import redSecundaria as rs
from Clase_dis_sec_v3_1 import cuadroTurno as ct


app = Flask(__name__)
api = Api(app)
cors = CORS(app)
parser = reqparse.RequestParser()

class REST(Resource):
  def post (self):
    parser.add_argument('padron', type=str)
    parser.add_argument('refuerzos', type=str)
    parser.add_argument('solicitud', type=str)
    parser.add_argument('reservorio', type=str)
    parser.add_argument('modos', type=str)
    parser.add_argument('caudal', type=int)
    parser.add_argument('turno', type=int)
    parser.add_argument('fecha', type=str)
    parser.add_argument('simular', type=int)
    parser.add_argument('vol_riego_p_ha', type=int)
    args = parser.parse_args()
    
    #Parametros para conformar la red secundaria
    padron = args['padron']
    refuerzo = args['refuerzos']
    solicitud = args['solicitud']
    reservorio = args['reservorio']
    modos = args['modos']
    caudal_cabecera = args['caudal']
    dur_turno = args['turno']
    fecha_inicio = args['fecha']
    simular = args['simular']

    #Parametros para el turnado
    vol_riego_p_ha = args['vol_riego_p_ha']

    red = rs(padron=padron,
             refuerzo=refuerzo,
             solicitud=solicitud,
             reservorio=reservorio,
             modos=modos,
             caudal_canal=caudal_cabecera,
             dur_turno=dur_turno,
             fecha_inicio=fecha_inicio,
             vol_riego_p_ha=vol_riego_p_ha,
             simular = simular
             )

    turno = {}
    cuadro_turno = {}
    cuenta_agua = {}

    for cauce, datos in self.cauces_g:
      turno[cauce] = ct(padron=red.get_subpadron()[cauce],
                        inicio=red.set_modo_riego().inicio_c[cauce],
                        volumen=red.get_subpadron()[cauce].sup_riego * red.get_vol_riego_ha(),
                        tiempo=red.get_subpadron()[cauce].sup_riego * (red.set_modo_riego().turnado_c[1:] / red.cauces_g.sup_riego.sum())[cauce],
                        caudal=red.get_caudal_riego()[cauce],
                        vol_riego_p_ha=vol_riego_p_ha
                        )

      cuadro_turno[cauce] = turno[cauce].set_turno_riego()
      cuenta_agua[cauce] = turno[cauce].set_cuenta_agua()
    
    #Genera salidas de datos
    cuadro = cuadro_turno
    cuentaAgua = cuenta_agua
    cuadroCaudales = pd.DataFrame({'Caudal':red.get_caudal_riego()[1:],
                                   'Tpo de Turnado':pd.to_timedelta(red.set_modo_riego().turnado_c[1:], unit='d'),
                                   'Sup de Riego':red.get_sup_riego().cauce}).to_json(orient = 'index')
    cuadroGeneral = pd.DataFrame({'Sup empadronada': red.cauces_g.sup_emp_reducida.sum(),
                          'Sup de distribucion': red.cauces_g.sup_riego.sum() - (red.cauces_g.sup_anexa.sum() + red.cauces_g.sup_pase.sum()),
                          'Sup de riego': red.cauces_g.sup_riego.sum(),
                          'Ctd de padrones': red.cauces_g.PP.count(),
                          'Tiempo de red': red.get_tpo_red(),
                          'Tpo x ha': pd.to_timedelta(red.set_modo_riego().turnado_c[1:] / red.cauces_g.sup_riego.sum(),unit='d'),
                          'Inicio': red.set_modo_riego().inicio_c[1:],
                          'Duracion': pd.to_timedelta(red.set_modo_riego().turnado_c[1:], unit='d'),
                          'Fin': red.set_modo_riego().inicio_c[1:]+pd.to_timedelta(red.set_modo_riego().turnado_c[1:], unit='d'),
                          'Vol x ha': red.get_vol_riego().cauce / red.cauces_g.sup_riego.sum(),
                          'Volumen': red.get_vol_riego().cauce,
                          #'Compensacion': red.cauces_g.fc.sum(),
                          'Coef de riego': red.get_caudal_riego()[1:] / red.cauces_g.sup_riego.sum(),
                          'Caudal': red.get_caudal_riego()[1:]
                          }).to_json(orient = 'index')
    
    #Compone los datos para la vista
    response = { "cuentaAgua": cuentaAgua, "cuadro": cuadro, "caudales": cuadroCaudales, "dashboard": cuadroGeneral}
    return response

api.add_resource(REST, '/turno_riego')

class Helloworld(Resource):
  def get(self):
    return { "data": "Hola vieja" }
api.add_resource(Helloworld, '/hola')

# Solo en modo desarrollo
if __name__ == "__main__":
  app.run(debug=True)
