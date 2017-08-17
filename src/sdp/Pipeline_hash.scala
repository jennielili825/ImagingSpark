
package test
import java.nio.ByteBuffer

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.{UnionRDD, RDD}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import scala.util.hashing.MurmurHash3

object Pipeline_hash {

    type Data = Array[Byte]

    var scale_data : Double = 0.20;
    val scale_compute : Double = 1.0;

    
    def main(args : Array[String]) {
        val conf = new SparkConf().setAppName("SDP Pipeline")
        val sc = new SparkContext(conf)
        scale_data=args(0).toDouble
        var rdd_count = 0

        //  Tsnap       = 85.1 s
        //  Nfacet      = 7x7
        //  Nf_max      = 65536 (using 1638)
        //  Tobs        = 3600.0 s (using 85.7 s)
        //  Nmajortotal = 9 (using 1)
        // === Extract_LSM ===
        val extract_lsm : HashMap[(Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val result_ix = Tuple2(beam, major_loop)
            result += Tuple2(result_ix, sc.parallelize(List(result_ix)).map(extract_lsm_kernel))
            rdd_count += 1
            result
        }
        // === Local Sky Model ===
        val local_sky_model : HashMap[Unit, RDD[Data]] = {
            val result = HashMap[Unit, RDD[Data]]()
            val result_ix = ()
            result += Tuple2(result_ix, sc.parallelize(List(result_ix)).map(local_sky_model_kernel))
            rdd_count += 1
            result
        }
        // === Telescope Management ===
        val telescope_management : HashMap[Unit, RDD[Data]] = {
            val result = HashMap[Unit, RDD[Data]]()
            val result_ix = ()
            result += Tuple2(result_ix, sc.parallelize(List(result_ix)).map(telescope_management_kernel))
            rdd_count += 1
            result
        }
        // === Visibility Buffer ===
        val visibility_buffer : HashMap[(Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            for (frequency <- 0 until 20) {
                val time = 0
                val baseline = 0
                val polarisation = 0
                val result_ix = Tuple5(beam, frequency, time, baseline, polarisation)
                result += Tuple2(result_ix, sc.parallelize(List(result_ix)).map(visibility_buffer_kernel))
                rdd_count += 1
            }
            result
        }
        // === Reprojection Predict + IFFT ===
        val reppre_ifft : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            for (frequency <- 0 until 5) {
                val time = 0
                for (facet <- 0 until 49) {
                    for (polarisation <- 0 until 4) {
                        val dep_extract_lsm = ListBuffer[RDD[Data]]()
                        dep_extract_lsm += extract_lsm(Tuple2(beam, major_loop))
                        val result_ix = Tuple6(beam, major_loop, frequency, time, facet, polarisation)
                        result += Tuple2(result_ix, sc.union(dep_extract_lsm).repartition(1).glom().map((result_ix,_)).map(reppre_ifft_kernel))
                        rdd_count += 1
                    }
                }
            }
            result
        }
        // === Telescope Data ===
        val telescope_data : HashMap[(Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val frequency = 0
            val time = 0
            val baseline = 0
            val dep_telescope_management = ListBuffer[RDD[Data]]()
            dep_telescope_management += telescope_management(())
            val result_ix = Tuple4(beam, frequency, time, baseline)
            result += Tuple2(result_ix, sc.union(dep_telescope_management).repartition(1).glom().map((result_ix,_)).map(telescope_data_kernel))
            rdd_count += 1
            result
        }
        // === Degridding Kernel Update + Degrid ===
        val degkerupd_deg : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            for (frequency <- 0 until 20) {
                val time = 0
                for (facet <- 0 until 49) {
                    for (polarisation <- 0 until 4) {
                        val dep_telescope_data = ListBuffer[RDD[Data]]()
                        dep_telescope_data += telescope_data(Tuple4(beam, (frequency*1/20), time, 0))
                        val dep_reppre_ifft = ListBuffer[RDD[Data]]()
                        dep_reppre_ifft += reppre_ifft(Tuple6(beam, major_loop, (frequency*5/20), time, facet, polarisation))
                        val result_ix = Tuple6(beam, major_loop, frequency, time, facet, polarisation)
                        result += Tuple2(result_ix, sc.union(dep_telescope_data ++ dep_reppre_ifft).repartition(1).glom().map((result_ix,_)).map(degkerupd_deg_kernel))
                        rdd_count += 1
                    }
                }
            }
            result
        }
        // === Phase Rotation Predict + DFT + Sum visibilities ===
        val pharotpre_dft_sumvis : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            for (frequency <- 0 until 20) {
              
              
                val time = 0
                val baseline = 0
                val polarisation = 0
                val dep_degkerupd_deg = ListBuffer[RDD[Data]]()
                for (i_facet <- 0 until 49) {
                    for (i_polarisation <- 0 until 4) {
                        dep_degkerupd_deg += degkerupd_deg(Tuple6(beam, major_loop, frequency, time, i_facet, (polarisation*4/1)+i_polarisation))
                    }
                }
                val dep_extract_lsm = ListBuffer[RDD[Data]]()
                dep_extract_lsm += extract_lsm(Tuple2(beam, major_loop))
                val result_ix = Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
                result += Tuple2(result_ix, sc.union(dep_degkerupd_deg ++ dep_extract_lsm).repartition(1).glom().map((result_ix,_)).map(pharotpre_dft_sumvis_kernel))
                rdd_count += 1
                
                
                
            }
            result
        }
        // === Timeslots ===
        val timeslots : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            for (time <- 0 until 8) {
                val baseline = 0
                val polarisation = 0
                val dep_visibility_buffer = ListBuffer[RDD[Data]]()
                for (i_frequency <- 0 until 20) {
                    dep_visibility_buffer += visibility_buffer(Tuple5(beam, (frequency*20/1)+i_frequency, (time*1/8), baseline, polarisation))
                }
                val dep_pharotpre_dft_sumvis = ListBuffer[RDD[Data]]()
                for (i_frequency <- 0 until 20) {
                    dep_pharotpre_dft_sumvis += pharotpre_dft_sumvis(Tuple6(beam, major_loop, (frequency*20/1)+i_frequency, (time*1/8), baseline, polarisation))
                }
                val result_ix = Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
                result += Tuple2(result_ix, sc.union(dep_visibility_buffer ++ dep_pharotpre_dft_sumvis).repartition(1).glom().map((result_ix,_)).map(timeslots_kernel))
                rdd_count += 1
            }
            result
        }
        // === Solve ===
        val solve : HashMap[(Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            for (time <- 0 until 8) {
                val polarisation = 0
                val dep_timeslots = ListBuffer[RDD[Data]]()
                dep_timeslots += timeslots(Tuple6(beam, major_loop, frequency, time, 0, polarisation))
                val result_ix = Tuple5(beam, major_loop, frequency, time, polarisation)
                result += Tuple2(result_ix, sc.union(dep_timeslots).repartition(1).glom().map((result_ix,_)).map(solve_kernel))
                rdd_count += 1
            }
            result
        }
        // === Correct + Subtract Visibility + Flag ===
        val cor_subvis_flag : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            for (frequency <- 0 until 20) {
                val time = 0
                val baseline = 0
                val polarisation = 0
                val dep_solve = ListBuffer[RDD[Data]]()
                for (i_time <- 0 until 8) {
                    dep_solve += solve(Tuple5(beam, major_loop, (frequency*1/20), (time*8/1)+i_time, polarisation))
                }
                val dep_visibility_buffer = ListBuffer[RDD[Data]]()
                dep_visibility_buffer += visibility_buffer(Tuple5(beam, frequency, time, baseline, polarisation))
                val dep_pharotpre_dft_sumvis = ListBuffer[RDD[Data]]()
                dep_pharotpre_dft_sumvis += pharotpre_dft_sumvis(Tuple6(beam, major_loop, frequency, time, baseline, polarisation))
                val result_ix = Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
                result += Tuple2(result_ix, sc.union(dep_solve ++ dep_visibility_buffer ++ dep_pharotpre_dft_sumvis).repartition(1).glom().map((result_ix,_)).map(cor_subvis_flag_kernel))
                rdd_count += 1
            }
            result
        }
        // === Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection ===
        val grikerupd_pharot_grid_fft_rep : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            val time = 0
            for (facet <- 0 until 49) {
                for (polarisation <- 0 until 4) {
                    val dep_telescope_data = ListBuffer[RDD[Data]]()
                    dep_telescope_data += telescope_data(Tuple4(beam, frequency, time, 0))
                    val dep_cor_subvis_flag = ListBuffer[RDD[Data]]()
                    for (i_frequency <- 0 until 20) {
                        dep_cor_subvis_flag += cor_subvis_flag(Tuple6(beam, major_loop, (frequency*20/1)+i_frequency, time, 0, (polarisation*1/4)))
                    }
                    val result_ix = Tuple6(beam, major_loop, frequency, time, facet, polarisation)
                    result += Tuple2(result_ix, sc.union(dep_telescope_data ++ dep_cor_subvis_flag).repartition(1).glom().map((result_ix,_)).map(grikerupd_pharot_grid_fft_rep_kernel))
                    rdd_count += 1
                }
            }
            result
        }
        // === Sum Facets ===
        val sum_facets : HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            val time = 0
            for (facet <- 0 until 49) {
                for (polarisation <- 0 until 4) {
                    val dep_grikerupd_pharot_grid_fft_rep = ListBuffer[RDD[Data]]()
                    dep_grikerupd_pharot_grid_fft_rep += grikerupd_pharot_grid_fft_rep(Tuple6(beam, major_loop, frequency, time, facet, polarisation))
                    val result_ix = Tuple6(beam, major_loop, frequency, time, facet, polarisation)
                    result += Tuple2(result_ix, sc.union(dep_grikerupd_pharot_grid_fft_rep).repartition(1).glom().map((result_ix,_)).map(sum_facets_kernel))
                    rdd_count += 1
                }
            }
            result
        }
        // === Identify Component ===
        val identify_component : HashMap[(Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            for (facet <- 0 until 49) {
                val dep_sum_facets = ListBuffer[RDD[Data]]()
                for (i_polarisation <- 0 until 4) {
                    dep_sum_facets += sum_facets(Tuple6(beam, major_loop, frequency, 0, facet, i_polarisation))
                }
                val result_ix = Tuple4(beam, major_loop, frequency, facet)
                result += Tuple2(result_ix, sc.union(dep_sum_facets).repartition(1).glom().map((result_ix,_)).map(identify_component_kernel))
                rdd_count += 1
            }
            result
        }
        // === Source Find ===
        val source_find : HashMap[(Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val dep_identify_component = ListBuffer[RDD[Data]]()
            for (i_facet <- 0 until 49) {
                dep_identify_component += identify_component(Tuple4(beam, major_loop, 0, i_facet))
            }
            val result_ix = Tuple2(beam, major_loop)
            result += Tuple2(result_ix, sc.union(dep_identify_component).repartition(1).glom().map((result_ix,_)).map(source_find_kernel))
            rdd_count += 1
            result
        }
        // === Subtract Image Component ===
        val subimacom : HashMap[(Int,Int,Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int,Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val frequency = 0
            for (facet <- 0 until 49) {
                val dep_identify_component = ListBuffer[RDD[Data]]()
                dep_identify_component += identify_component(Tuple4(beam, major_loop, frequency, facet))
                val dep_sum_facets = ListBuffer[RDD[Data]]()
                for (i_polarisation <- 0 until 4) {
                    dep_sum_facets += sum_facets(Tuple6(beam, major_loop, frequency, 0, facet, i_polarisation))
                }
                val result_ix = Tuple4(beam, major_loop, frequency, facet)
                result += Tuple2(result_ix, sc.union(dep_identify_component ++ dep_sum_facets).repartition(1).glom().map((result_ix,_)).map(subimacom_kernel))
                rdd_count += 1
            }
            result
        }
        // === Update LSM ===
        val update_lsm : HashMap[(Int,Int), RDD[Data]] = {
            val result = HashMap[(Int,Int), RDD[Data]]()
            val beam = 0
            val major_loop = 0
            val dep_local_sky_model = ListBuffer[RDD[Data]]()
            dep_local_sky_model += local_sky_model(())
            val dep_source_find = ListBuffer[RDD[Data]]()
            dep_source_find += source_find(Tuple2(beam, major_loop))
            val result_ix = Tuple2(beam, major_loop)
            result += Tuple2(result_ix, sc.union(dep_local_sky_model ++ dep_source_find).repartition(1).glom().map((result_ix,_)).map(update_lsm_kernel))
            rdd_count += 1
            result
        }
        // === Terminate ===
        val dep_subimacom = ListBuffer[RDD[Data]]()
        for (i_facet <- 0 until 49) {
            dep_subimacom += subimacom(Tuple4(0, 0, 0, i_facet))
        }
        val dep_update_lsm = ListBuffer[RDD[Data]]()
        dep_update_lsm += update_lsm(Tuple2(0, 0))
        println(f"Finishing ${rdd_count}%d RDDs...")
        for (dep <- dep_subimacom) {
            println(f"Subtract Image Component: ${dep.count()}%d")
        }
        for (dep <- dep_update_lsm) {
            println(f"Update LSM: ${dep.count()}%d")
        }
        sc.stop()
    }
    def extract_lsm_kernel : ((Int,Int)) => Data = { case ix =>
        var label : String = "Extract_LSM (0.0 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 0L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def local_sky_model_kernel : (Unit) => Data = { case ix =>
        var label : String = "Local Sky Model (0.0 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 0L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def telescope_management_kernel : (Unit) => Data = { case ix =>
        var label : String = "Telescope Management (0.0 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 0L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def visibility_buffer_kernel : ((Int,Int,Int,Int,Int)) => Data = { case ix =>
        var label : String = "Visibility Buffer (46168.5 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 153895035L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def reppre_ifft_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Reprojection Predict + IFFT (24209.8 MB, 0.39 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 80699223L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def telescope_data_kernel : (((Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Telescope Data (0.0 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 0L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def degkerupd_deg_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Degridding Kernel Update + Degrid (88.8 MB, 0.07 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 296108L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def pharotpre_dft_sumvis_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Phase Rotation Predict + DFT + Sum visibilities (46168.5 MB, 131.08 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 153895035L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def timeslots_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Timeslots (1518.3 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 5060952L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def solve_kernel : (((Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Solve (8262.8 MB, 2939.73 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 27542596L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def cor_subvis_flag_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Correct + Subtract Visibility + Flag (46639.6 MB, 1.24 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 155465392L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def grikerupd_pharot_grid_fft_rep_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection (24208.9 MB, 2.34 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 80696289L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def sum_facets_kernel : (((Int,Int,Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Sum Facets (24208.9 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 80696289L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def identify_component_kernel : (((Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Identify Component (0.2 MB, 3026.11 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 533L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def source_find_kernel : (((Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Source Find (5.8 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 19200L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def subimacom_kernel : (((Int,Int,Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Subtract Image Component (121044.4 MB, 67.14 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 403481447L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
    def update_lsm_kernel : (((Int,Int), Array[Data])) => Data = { case (ix, all_data) =>
        var label : String = "Update LSM (0.0 MB, 0.00 Tflop) "+ix.toString
        var hash : Int = MurmurHash3.stringHash(label)
        var input_size : Long = 0
        for (data <- all_data) {
          hash ^= MurmurHash3.bytesHash(data.slice(0,4))
          input_size += data.length
        }
        println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size/1000000).toString() + " MB input)")
        val result = new Array[Byte](math.max(4, (scale_data * 0L).toInt))
        ByteBuffer.wrap(result).putInt(0, hash)
        result
    }
    
}
//  * 5507 tasks
//  * 141.90 GB produced (reduced from 42.57 TB, factor 300)
//  * 180.57 Pflop represented
// This is roughly(!):
//  * 485.39 min node time (6.20 Tflop/s effective)
//  * 515.90 s island time (0.35 Pflop/s effective)
//  * 12.90 s cluster time (14.00 Pflop/s effective)
//  * 0.358% SKA SDP
//  * 0.00119% SKA SDP internal data rate
