/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gcp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tomcat.jni.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static void main(String[] args) {

		// TODO Pasar los parametros por codigo

		// Start by defining the options for the pipeline.
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);

		// Leemos las filas de la tabla
		PCollection<TableRow> tabla1 = p.apply(BigQueryIO.readTableRows().from("third-crossing-236813:Prueba.Tabla1"));

		PCollection<String> tabla1String = tabla1.apply("to_string", ParDo.of(new ProcesarFilasAString()));

		tabla1String.apply(TextIO.write().to("c:/users/Carlos/file").withSuffix(".txt"));

		p.run();
	}

	/**
	 * Procesador que transforma filas de BigQuery (TableRow) en String
	 * Contiene un metido etiquetado @ProcessElement que es el que trabaja 1 a 1 
	 * los elementos de la PCollection sobre la que se ejecuta.
	 * De entrada recibe una fila, etiquetada como @Element y de salida un String en el OutpurReciever
	 * @author Carlos
	 *
	 */
	private static class ProcesarFilasAString extends DoFn<TableRow, String> {

		@ProcessElement
		public void processElement(@Element TableRow fila, OutputReceiver<String> filaString) {
			
			//Filtro todos los nombres que empiecen por A
			if (!((String)fila.get("nombre")).toUpperCase().startsWith("A")) {
				filaString.output(fila.toString());
			}

		}

	}
}
