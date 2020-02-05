/*
 ************************************************************************************
 * Copyright (C) 2009-2017 Openbravo S.L.U.
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 * or in the legal folder of this module distribution.
 ************************************************************************************
 */

package org.openbravo.module.idljava.proc;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;

import org.openbravo.base.exception.OBException;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.Utility;
import org.openbravo.idl.proc.IdlService;
import org.openbravo.idl.proc.Parameter;
import org.openbravo.idl.proc.Validator;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 * @author adrian
 */
public abstract class IdlServiceJava extends IdlService {

  @Override
  protected boolean executeImport(String filename, boolean insert) throws Exception {
    CSVReader reader = null;
    try {
      reader = new CSVReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"), ',',
          '\"', '\\', 0, false, true);

      String[] nextLine;

      // Check header
      nextLine = reader.readNext();
      if (nextLine == null) {
        throw new OBException(Utility.messageBD(conn, "IDLJAVA_HEADER_MISSING", vars.getLanguage()));
      }
      Parameter[] parameters = getParameters();
      if (parameters.length > nextLine.length) {
        throw new OBException(Utility.messageBD(conn, "IDLJAVA_HEADER_BAD_LENGTH",
            vars.getLanguage()));
      }

      Validator validator;

      int i=0;
      long tiempoAcumulado = 0;
      long startDate = new Date().getTime();
      while ((nextLine = reader.readNext()) != null) {

        if (nextLine.length > 1 || nextLine[0].length() > 0) {
          // It is not an empty line

          // Validate types
          if (parameters.length > nextLine.length) {
            throw new OBException(Utility.messageBD(conn, "IDLJAVA_LINE_BAD_LENGTH",
                vars.getLanguage()));
          }

          validator = getValidator(getEntityName());
          //System.out.println("Inicio de validacion: "+startDate);
          Object[] result = validateProcess(validator, nextLine);
          //long endDate = new Date().getTime()-startDate;
          //System.out.println("Fin de validacion: "+endDate);
          //System.out.println("Numero de linea: "+i);
          if ("0".equals(validator.getErrorCode())) {
            finishRecordProcess(result);
          } else {
            logRecordError(validator.getErrorMessage(), result);
          }
          long endProcess = new Date().getTime()-startDate;
          //System.out.println("Fin de proceso: "+endProcess);
          tiempoAcumulado+=endProcess;
          i++;
          if (++i % 1000 == 0) {
			OBDal.getInstance().flush();
			OBDal.getInstance().getSession().clear();
		  }
          //System.out.println("Registros gestionados: "+i);
          //System.out.println("Tiempo acumulado: "+tiempoAcumulado);
        }
      }
      System.out.println("Tiempo final: "+tiempoAcumulado);

      return true;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  protected abstract String getEntityName();

  protected abstract Object[] validateProcess(Validator validator, String... values)
      throws Exception;
}
