package test
import java.nio.ByteBuffer
import alluxio.AlluxioURI
import org.apache.spark.{ Partitioner,SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import alluxio.client.file.options.CreateFileOptions
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import alluxio.client.file.FileSystem
import alluxio.client.WriteType
import alluxio.client.ReadType
import alluxio.client.file.options.OpenFileOptions
import alluxio.Configuration
import alluxio.Constants
import alluxio.PropertyKey
import org.apache.spark.broadcast.Broadcast
object Pipeline_alluxio {
  class SDPPartitioner_pharo_alluxio(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.split(',')(2).toInt
  }
}
  class SDPPartitioner(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.split(',')(3).toInt
  }
}
  type Data = Array[Byte]
  val scale_data: Double = 0.1;
  val scale_compute: Double = 1.0;
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SDP Pipeline")
    val sc = new SparkContext(conf)
    val extract_lsm: RDD[((Int, Int), Data)] = {
      val initset = ListBuffer[(Int, Int)]()
      val beam = 0
      val major_loop = 0
      initset += Tuple2(beam, major_loop)
      sc.parallelize(initset).map(extract_lsm_kernel)
    }
    // === Local Sky Model ===
    val local_sky_model: RDD[(Unit, Data)] = {
      val initset = ListBuffer[Unit]()
      initset += ()
      sc.parallelize(initset).map(local_sky_model_kernel)
    }
    // === Telescope Management ===
    val telescope_management: RDD[(Unit, Data)] = {
      val initset = ListBuffer[Unit]()
      initset += ()
      sc.parallelize(initset).map(telescope_management_kernel)
    }
    // === Visibility Buffer ===
    val visibility_buffer: RDD[(Int, Int, Int, Int, Int)] = {
      val initset = ListBuffer[(Int, Int, Int, Int, Int)]()
      val beam = 0
      for (frequency <- 0 until 20) {
        val time = 0
        val baseline = 0
        val polarisation = 0
        initset += Tuple5(beam, frequency, time, baseline, polarisation)
      }
      sc.parallelize(initset).map(visibility_buffer_kernel)
    }
    printf(" the size of new rdd visibility_buffer" + visibility_buffer.count())
    // === Telescope Data ===
    val telescope_data: RDD[((Int, Int, Int, Int), Data)] = {
      val dep_telescope_management = HashMap[Unit, ListBuffer[(Int, Int, Int, Int)]]()
      val beam = 0
      val frequency = 0
      val time = 0
      val baseline = 0
      dep_telescope_management.getOrElseUpdate((), ListBuffer()) += Tuple4(beam, frequency, time, baseline)
      val input_telescope_management: RDD[((Int, Int, Int, Int), Data)] =
        telescope_management.flatMap(ix_data => dep_telescope_management(ix_data._1).map((_, ix_data._2)))
      input_telescope_management.groupByKey().mapValues(Tuple1(_)).map(telescope_data_kernel)
    }
     var broads_input_telescope_data = sc.broadcast(telescope_data.collect())
     var broadcast_lsm=sc.broadcast(extract_lsm.collect())
     
      val reppre_ifft : RDD[(Int,Int,Int,Int,Int,Int)] = {
      var initset=ListBuffer[(Int, Int, Int, Int, Int,Int)]()
      val dep_extract_lsm = HashMap[(Int, Int), ListBuffer[(Int, Int, Int, Int, Int, Int)]]()
      val beam = 0
      val major_loop = 0
      for (frequency <- 0 until 5) {
        val time = 0
        for (facet <- 0 until 36) {
          for (polarisation <- 0 until 4) {
            //dep_extract_lsm.getOrElseUpdate(Tuple2(beam, major_loop), ListBuffer()) += Tuple6(beam, major_loop, frequency, time, facet, polarisation)
             initset += Tuple6(beam, major_loop, frequency, time, facet, polarisation)
          }
        }
      }
      sc.parallelize(initset).map(ix=>reppre_ifft_kernel(ix,broads_input_telescope_data,broadcast_lsm))
    }
     
     var degrid: RDD[((Int,Int,Int,Int,Int,Int), Data)] = {
      reppre_ifft.flatMap(ix=>degrid_kernel(ix,broads_input_telescope_data,broadcast_lsm))
    }
      degrid.cache()
      val pharotpre_dft_sumvis: RDD[(Int, Int, Int, Int, Int)] = {
      val dep_extract_lsm = HashMap[(Int, Int), ListBuffer[(Int, Int, Int, Int, Int, Int)]]()
      val dep_degkerupd_deg = HashMap[(Int, Int, Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int, Int, Int)]]()
      val initset = ListBuffer[(Int, Int, Int, Int, Int)]()
      val beam = 0
      for (frequency <- 0 until 20) {
        val time = 0
        val baseline = 0
        val polarisation = 0
        initset += Tuple5(beam, frequency, time, baseline, polarisation)
      }
      degrid.partitionBy(new SDPPartitioner_pharo_alluxio(20)).mapPartitions(pharotpre_dft_sumvis_kernel2)

    }
      // The count is necessary, because Spark is lazy and the pharotpre_dft_sumvis stage should be trigered.
    printf(" the size of new rdd pharotpre_dft_sumvis" + pharotpre_dft_sumvis.count())
    // === Timeslots ===
    val timeslots: RDD[((Int, Int, Int, Int, Int, Int), Data)] = {
      val initset = ListBuffer[(Int, Int, Int, Int, Int, Int)]()
      val beam = 0
      for (time <- 0 until 6) {
        val frequency = 0
        val baseline = 0
        val polarisation = 0
        val major_loop = 0
        initset += Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
      }
      sc.parallelize(initset).map(timeslots_kernel)

    }
    val solve: RDD[(Int, Int, Int, Int, Int)] = {
      val dep_timeslots = HashMap[(Int, Int, Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int, Int)]]()
      val beam = 0
      val major_loop = 0
      val frequency = 0
      for (time <- 0 until 6) {
        val polarisation = 0
        dep_timeslots.getOrElseUpdate(Tuple6(beam, major_loop, frequency, time, 0, polarisation), ListBuffer()) += Tuple5(beam, major_loop, frequency, time, polarisation)
      }
      val input_timeslots: RDD[((Int, Int, Int, Int, Int), Data)] =
        timeslots.flatMap(ix_data => dep_timeslots(ix_data._1).map((_, ix_data._2)))
      input_timeslots.groupByKey().mapValues(Tuple1(_)).map(solve_kernel)
    }
    printf("solve    count  "+solve.count())
    // === Correct + Subtract Visibility + Flag ===
    val cor_subvis_flag: RDD[((Int, Int, Int, Int, Int, Int),Data)] = {
      val initset = ListBuffer[(Int, Int, Int, Int, Int, Int)]()
      val beam = 0
      for (frequency <- 0 until 20) {
        val time = 0
        val baseline = 0
        val polarisation = 0
        val major_loop = 0
        initset += Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
      }
      sc.parallelize(initset).map(cor_subvis_flag_kernel)

    }
    cor_subvis_flag.cache()
    var broads_input1 = sc.broadcast(cor_subvis_flag.collect())
    // === Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection ===
    val grikerupd_pharot_grid_fft_rep: RDD[((Int, Int, Int, Int, Int, Int), Data)] = {

      val initset = ListBuffer[(Int, Int, Int, Int, Int, Int)]()
      val beam = 0
      var frequency = 0
      for (facet <- 0 until 36) {
        for (polarisation <- 0 until 4) {
          val time = 0
          val major_loop = 0
          initset += Tuple6(beam, major_loop, frequency, time, facet, polarisation)
        }
      }
      sc.parallelize(initset).map(ix => grikerupd_pharot_grid_fft_rep_kernel(ix, broads_input_telescope_data, broads_input1))
    }
    printf(" the size of new rdd  grikerupd_pharot_grid_fft_rep" + grikerupd_pharot_grid_fft_rep.count())
    // === Sum Facets ===
    val sum_facets: RDD[((Int, Int, Int, Int, Int, Int), Data)] = {
      val initset = ListBuffer[(Int, Int, Int, Int, Int, Int)]()
      val beam = 0
      var frequency = 0
      for (facet <- 0 until 36) {
        for (polarisation <- 0 until 4) {
          val time = 0
          val major_loop = 0
          initset += Tuple6(beam, major_loop, frequency, time, facet, polarisation)
        }
      }
      grikerupd_pharot_grid_fft_rep.map(sum_facets_kernel)
    }
    printf(" the size of new rdd sum_facets" + sum_facets.count())
    sum_facets.cache()
    // === Identify Component ===
    val identify_component: RDD[((Int, Int, Int, Int), Data)] = {
      val dep_sum_facets = HashMap[(Int, Int, Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int)]]()
      val beam = 0
      val major_loop = 0
      val frequency = 0
      for (facet <- 0 until 36) {
        for (i_polarisation <- 0 until 4) {
          dep_sum_facets.getOrElseUpdate(Tuple6(beam, major_loop, frequency, 0, facet, i_polarisation), ListBuffer()) += Tuple4(beam, major_loop, frequency, facet)
        }
      }

   
      sum_facets.partitionBy(new SDPPartitioner(36)).mapPartitions(identify_component_kernel_partitions)
    }
    var broads_input2 = sc.broadcast(identify_component.collect())

    // === Source Find ===
    val source_find: RDD[((Int, Int), Data)] = {
      val dep_identify_component = HashMap[(Int, Int, Int, Int), ListBuffer[(Int, Int)]]()
      val beam = 0
      val major_loop = 0
      for (i_facet <- 0 until 36) {
        dep_identify_component.getOrElseUpdate(Tuple4(beam, major_loop, 0, i_facet), ListBuffer()) += Tuple2(beam, major_loop)
      }
      val input_identify_component: RDD[((Int, Int), Data)] =
        identify_component.flatMap(ix_data => dep_identify_component(ix_data._1).map((_, ix_data._2)))
      input_identify_component.groupByKey().mapValues(Tuple1(_)).map(source_find_kernel)
    }
    //  printf(" the size of new rdd source_find" + source_find.count())
    // === Subtract Image Component ===
    val subimacom: RDD[((Int, Int, Int, Int, Int), Data)] = {
      val dep_identify_component = HashMap[(Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int, Int)]]()
      val dep_sum_facets = HashMap[(Int, Int, Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int, Int)]]()
      val beam = 0
      val major_loop = 0
      val frequency = 0
      for (facet <- 0 until 36) {
        for (polarisation <- 0 until 4) {
          dep_identify_component.getOrElseUpdate(Tuple4(beam, major_loop, frequency, facet), ListBuffer()) += Tuple5(beam, major_loop, frequency, facet, polarisation)
          dep_sum_facets.getOrElseUpdate(Tuple6(beam, major_loop, frequency, 0, facet, polarisation), ListBuffer()) += Tuple5(beam, major_loop, frequency, facet, polarisation)
        }
      }
      //   val input_identify_component: RDD[((Int, Int, Int, Int, Int), Data)] =
      //   identify_component.flatMap(ix_data => dep_identify_component(ix_data._1).map((_, ix_data._2)))
      // val input_sum_facets: RDD[((Int, Int, Int, Int, Int), Data)] =
      // sum_facets.flatMap(ix_data => dep_sum_facets(ix_data._1).map((_, ix_data._2)))
      sum_facets.map(ix => subimacom_kernel(ix, broads_input2))
    }
    // === Update LSM ===
    val update_lsm: RDD[((Int, Int), Data)] = {
      val dep_local_sky_model = HashMap[Unit, ListBuffer[(Int, Int)]]()
      val dep_source_find = HashMap[(Int, Int), ListBuffer[(Int, Int)]]()
      val beam = 0
      val major_loop = 0
      dep_local_sky_model.getOrElseUpdate((), ListBuffer()) += Tuple2(beam, major_loop)
      dep_source_find.getOrElseUpdate(Tuple2(beam, major_loop), ListBuffer()) += Tuple2(beam, major_loop)
      val input_local_sky_model: RDD[((Int, Int), Data)] =
        local_sky_model.flatMap(ix_data => dep_local_sky_model(ix_data._1).map((_, ix_data._2)))
      val input_source_find: RDD[((Int, Int), Data)] =
        source_find.flatMap(ix_data => dep_source_find(ix_data._1).map((_, ix_data._2)))
      input_local_sky_model.cogroup(input_source_find).map(update_lsm_kernel)
    }
    // === Terminate ===
    println("Finishing...")
    println(f"Update LSM: ${update_lsm.count()}%d")
    println(f"Subtract Image Component: ${subimacom.count()}%d")
  }
  def extract_lsm_kernel: ((Int, Int)) => ((Int, Int), Data) = {
    case ix =>
      var label: String = "Extract_LSM (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
    //  Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }

  def local_sky_model_kernel: (Unit) => (Unit, Data) = {
    case ix =>
      var label: String = "Local Sky Model (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
    //  Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }

  def telescope_management_kernel: (Unit) => (Unit, Data) = {
    case ix =>
      var label: String = "Telescope Management (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
    //  Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try { f(resource) } finally { resource.close() }

  def visibility_buffer_kernel: ((Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int)) = {
    case ix =>
      var label: String = "Visibility Buffer (36384.6 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
      //   System.setProperty("MASTER_HOSTNAME", "hadoop8")
      // System.setProperty("MASTER_RPC_PORT", "19998")
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      val path = new AlluxioURI("/visibility_buffer/" + ix._2)

      using(fs.createFile(path, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result)
      }
      (ix)

    /*  using(fs.openFile(path)) { in =>
      val buffer = new Array[Byte](1024)
      val size = in.read(buffer)
      System.out.println(new String(buffer, 0, size))
    }*/

  }
 def reppre_ifft_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int,Int,Int), Data)]],Broadcast[Array[((Int, Int), Data)]]) => (Int, Int, Int, Int, Int, Int) = {
    case (ix) =>
      //  here call original reppre_ifft_kernel to generate reppre_ifft data first 
      //get telescope data from Broadcast variable and combine  degkerupd_deg kernel here, generate degkerupd_deg data directly.
     // var mylist = new Array[((Int, Int, Int, Int, Int, Int),Data)](4)
      //after reppre_ifft we get the data
      val result2 = new Array[Byte](math.max(4, (scale_data * 329520000).toInt))
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get()
      var temp1 = ix._1._3 * 4
      val path = new AlluxioURI("/reppre_ifft/" + ix._1._1 + "_" + ix._1._2 + "_" + ix._1._3+ "_"+ ix._1._4+ "_"+ ix._1._5 + "_" + ix._1._6)
      using(fs.createFile(path, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result2)
      }
      (ix._1)
     
      
  }
 /* def reppre_ifft_kernel: (((Int, Int, Int, Int, Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int, Int, Int, Int, Int)) = {
    case (ix, Tuple1(data_extract_lsm)) =>
      // 
      //  here call original reppre_ifft_kernel to generate reppre_ifft data first 
      //get telescope data and combine  degkerupd_deg kernel here, generate degkerupd_deg data directly.
      //because telescope data is quite small, it can be stored in database or in a local cache, we omit the access time
      val result2 = new Array[Byte](math.max(4, (scale_data * 329520000).toInt))
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      System.out.println(" path   is    " + ix._2)
      var temp1 = ix._3 * 4
      val path = new AlluxioURI("/reppre_ifft/" + ix._1 + "_" + ix._2 + "_" + ix._3+ "_"+ ix._4+ "_"+ ix._5 + "_" + ix._6)
      using(fs.createFile(path, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result2)
      }
      (ix)

  }*/

  def telescope_data_kernel: (((Int, Int, Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int, Int, Int), Data) = {
    case (ix, Tuple1(data_telescope_management)) =>
      var label: String = "Telescope Data (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_telescope_management) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
   //   Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }
  
  def degrid_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int,Int,Int), Data)]],Broadcast[Array[((Int, Int), Data)]]) => TraversableOnce[(((Int, Int, Int, Int, Int, Int),Data))] = {
    case (ix) =>
      //  here call original reppre_ifft_kernel to generate reppre_ifft data first 
      //get telescope data from Broadcast variable and combine  degkerupd_deg kernel here, generate degkerupd_deg data directly.
      //  here call original reppre_ifft_kernel to generate reppre_ifft data first 
      //get reppre_fft from alluxio
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      //  var tempdata=new Array[Byte][36][4]
      var temp = ix._1._1 + "_" + ix._1._2 + "_" + ix._1._3+ "_"+ ix._1._4+ "_"+ ix._1._5 + "_" + ix._1._6
          val path = new AlluxioURI("/reppre_ifft/" + temp)
          using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in =>
            var buf = new Array[Byte](in.remaining().toInt)
            var tempdata = in.read(buf)
            in.close
          }
      
      var mylist = new Array[((Int, Int, Int, Int, Int, Int),Data)](4)
      val result2 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_2 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_3 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_4 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
    
    var temp1 = ix._1._3 * 4
      mylist(0)=((ix._1._1,ix._1._2 ,temp1,ix._1._4,ix._1._5,ix._1._6),result2)
      var temp2 = ix._1._3  * 4+1 
      mylist(1)=((ix._1._1,ix._1._2 ,temp2,ix._1._4,ix._1._5,ix._1._6),result2)
      var temp3 =ix._1._3  * 4 + 2
      mylist(2)=((ix._1._1,ix._1._2 ,temp3,ix._1._4,ix._1._5,ix._1._6),result2)
      var temp4 = ix._1._3  * 4+3
       mylist(3)=((ix._1._1,ix._1._2 ,temp4,ix._1._4,ix._1._5,ix._1._6),result2)
      mylist
      
  }
  def degkerupd_deg_kernel: (((Int, Int, Int, Int, Int, Int), (Iterable[Data], Iterable[Data]))) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case (ix, (data_reppre_ifft, data_telescope_data)) =>
      var label: String = "Degridding Kernel Update + Degrid (99.9 MB, 0.14 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_reppre_ifft) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      for (data <- data_telescope_data) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
   //   Thread.sleep((scale_compute * 23).toInt)
      (ix, result)
  }
  
  def pharotpre_dft_sumvis_kernel_old: (((Int, Int, Int, Int, Int, Int), Data)) => (Int, Int, Int, Int, Int, Int) = {
    case (ix, tempdata) =>
      // def pharotpre_dft_sumvis_kernel : (((Int,Int,Int,Int,Int,Int), (Iterable[Data],Iterable[Data]))) => (Int,Int,Int,Int,Int,Int) = { case (ix, (data_extract_lsm,data_degkerupd_deg)) =>
      var label: String = "Phase Rotation Predict + DFT + Sum visibilities (36384.6 MB, 102.32 Tflop) " + ix.toString
      //read data from alluxio
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      //  var tempdata=new Array[Byte][36][4]
      println(" path   is    " + ix._2)
      for (i_facet <- 0 until 36)
        for (polarization <- 0 until 4) {
          var temp = ix._1 +"_"+ix._2 + "_" + ix._3 +ix._4 + "_" + i_facet + "_" + polarization
          val path = new AlluxioURI("/degkerupd_deg/" + temp)
          using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in =>
            var buf = new Array[Byte](in.remaining().toInt)
            var tempdata = in.read(buf)
            in.close
          }
        }
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
      (ix)
  }
 def pharotpre_dft_sumvis_kernel2: (Iterator[((Int, Int, Int, Int, Int, Int), Data)]) => (Iterator[(Int, Int, Int, Int, Int)]) = {
    case ix =>
      // def pharotpre_dft_sumvis_kernel : (((Int,Int,Int,Int,Int,Int), (Iterable[Data],Iterable[Data]))) => (Int,Int,Int,Int,Int,Int) = { case (ix, (data_extract_lsm,data_degkerupd_deg)) =>
      var label: String = "Phase Rotation Predict + DFT + Sum visibilities (36384.6 MB, 102.32 Tflop) " + ix.toString
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      if(ix.hasNext)
    {
         var temp=ix.next()
         var result2 = new Array[(Int, Int, Int, Int,Int)](1)
         result2(0)=(temp._1._1,temp._1._2,temp._1._3,temp._1._4,temp._1._6)
          
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
      //write the pair to alluxio
      val path3 = new AlluxioURI("/pharotpre_dft_sumvis/" + temp._1._3)
      using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result)
      } //using
         result2.iterator
         
     }
    else
    {
	      var result2 = new Array[(Int, Int, Int, Int,Int)](1)
	      result2(0)=((0,0,0,0,0))
	      result2.iterator
      
    }
     
     
  }

  def timeslots_kernel: ((Int, Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case ix =>

      //from alluxio get the info 
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      //  var tempdata=new Array[Byte][36][4]
      //     println(" path   is    "+ix._2)
      for (frequency <- 0 until 20) {
        var temp = frequency
        val path = new AlluxioURI("/pharotpre_dft_sumvis/" + temp)
        val path2 = new AlluxioURI("/visibility_buffer/" + temp)

        using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in =>
          var buf = new Array[Byte](in.remaining().toInt)
          var tempdata = in.read(buf)
          in.close
        }
        using(fs.openFile(path2, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in2 =>
          var buf = new Array[Byte](in2.remaining().toInt)
          var tempdata = in2.read(buf)
          in2.close
        }

        //  }
      } //for
      val result = new Array[Byte](math.max(4, (scale_data * 15182856).toInt))
      (ix, result)
  }
  def solve_kernel: (((Int, Int, Int, Int, Int), Tuple1[Iterable[Data]])) => (Int, Int, Int, Int, Int) = {
    case (ix, Tuple1(data_timeslots)) =>
      var label: String = "Solve (8262.8 MB, 2939.73 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      val result = new Array[Byte](math.max(4, (scale_data * 82627788).toInt))
      val path3 = new AlluxioURI("/solve/" + ix._4)
      using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result)
      } //using
      (ix)
  }

  def cor_subvis_flag_kernel: ((Int, Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int, Int),Data) = {
    case ix =>
      var label: String = "Correct + Subtract Visibility + Flag (36755.9 MB, 0.98 Tflop) " + ix.toString
      // read from alluxio
      var temp = ix._3
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      val path = new AlluxioURI("/pharotpre_dft_sumvis/" + temp)
      val path2 = new AlluxioURI("/visibility_buffer/" + temp)
      var in1 = fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
      var buf = new Array[Byte](in1.remaining().toInt)
      var tempdata = in1.read(buf)
      var in2 = fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
      var buf2 = new Array[Byte](in2.remaining().toInt)
      var tempdata2 = in2.read(buf2)
      //get data from solve
      for (time <- 0 until 6) {
        val pathTime = new AlluxioURI("/solve/" + time)
        var in11 = fs.openFile(pathTime, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
        var buf3 = new Array[Byte](in11.remaining().toInt)
        var tempdata3 = in1.read(buf3)
      }
      val result = new Array[Byte](math.max(4, (scale_data * 367559071).toInt))
      //write to alluxio  /cor_subvis_flag/
   //   val path3 = new AlluxioURI("/cor_subvis_flag/" + ix._3)
     // using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
       // out.write(result)
      //} //using
      (ix,result)
  }
  //def grikerupd_pharot_grid_fft_rep_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int, Int, Int), Data)]], Broadcast[HashMap[Int, Data]]) => ((Int, Int, Int, Int, Int, Int), Data) = {
  def grikerupd_pharot_grid_fft_rep_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int, Int, Int), Data)]], Broadcast[Array[((Int, Int, Int, Int,Int,Int), Data)]] ) => ((Int, Int, Int, Int, Int, Int), Data) = {
   case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix._1, result)

  }
/* def grikerupd_pharot_grid_fft_rep_kernel: ((Int, Int, Int, Int, Int, Int),  Broadcast[Array[((Int, Int, Int, Int), Data)]],Broadcast[Array[((Int, Int, Int, Int,Int,Int), Data)]]) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix._1, result)

  }*/
 
  /*def grikerupd_pharot_grid_fft_rep_kernel: ((Int, Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case (ix) =>
      var label: String = "Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection (32951.0 MB, 3.81 Tflop) " + ix.toString
      //get cor_subvis_flag 
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "hadoop8");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      for (frequency <- 0 until 20) {
        val path = new AlluxioURI("/cor_subvis_flag/" + frequency)
        var in = fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
        var buf = new Array[Byte](in.remaining().toInt)
        var tempdata = in.read(buf)
      }
      //get telescope data, here omit it

      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix, result)
  }*/
  def sum_facets_kernel: (((Int, Int, Int, Int, Int, Int), Data)) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case (ix) =>
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix._1, result)
  }
 /* def sum_facets_kernel: (((Int, Int, Int, Int, Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case (ix, Tuple1(data_grikerupd_pharot_grid_fft_rep)) =>
      var label: String = "Sum Facets (32951.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_grikerupd_pharot_grid_fft_rep) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
   //   Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }*/
 def identify_component_kernel_partitions: (Iterator[((Int, Int, Int, Int, Int, Int), Data)]) => Iterator[((Int, Int, Int, Int), Data)] = {
    case (ix) =>
      val result = new Array[Byte](math.max(4, (scale_data * 1600).toInt))
      var result2 = new Array[((Int, Int, Int, Int), Data)](1)

      if (ix.hasNext) {
        var temp = ix.next()
        var result2 = new Array[((Int, Int, Int, Int), Data)](1)
        result2(0) = ((temp._1._1, temp._1._2, temp._1._3, temp._1._5), result)
        result2.iterator

      } else {
        // The condition is never satisfied
        var result2 = new Array[((Int, Int, Int, Int), Data)](1)
        result2(0) = ((0, 0, 0, 0), result)
        result2.iterator

      }

  }
  def identify_component_kernel: (((Int, Int, Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int, Int, Int), Data) = {
    case (ix, Tuple1(data_sum_facets)) =>
      var label: String = "Identify Component (0.2 MB, 4118.87 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_sum_facets) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 1600).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
   //   Thread.sleep((scale_compute * 664334).toInt)
      (ix, result)
  }

  def source_find_kernel: (((Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int), Data) = {
    case (ix, Tuple1(data_identify_component)) =>
      var label: String = "Source Find (5.8 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_identify_component) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 57600).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
   //   Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }

  /*def subimacom_kernel: (((Int, Int, Int, Int, Int), (Iterable[Data], Iterable[Data]))) => ((Int, Int, Int, Int, Int), Data) = {
    case (ix, (data_identify_component, data_sum_facets)) =>
      var label: String = "Subtract Image Component (167.9 MB, 4118.87 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_identify_component) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      for (data <- data_sum_facets) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 1678540).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
     // Thread.sleep((scale_compute * 664334).toInt)
      (ix, result)
  }*/
  def subimacom_kernel: (((Int, Int, Int, Int, Int, Int), Data), Broadcast[Array[((Int, Int, Int, Int), Data)]]) => ((Int, Int, Int, Int, Int), Data) = {
    case (ix) =>
      var label: String = "Subtract Image Component (167.9 MB, 4118.87 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0

      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 1678540).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
      //  Thread.sleep((scale_compute * 664334).toInt)
      var aa = ix._1._1
      ((aa._1, aa._2, aa._3, aa._5, aa._6), result)
  }


  def update_lsm_kernel: (((Int, Int), (Iterable[Data], Iterable[Data]))) => ((Int, Int), Data) = {
    case (ix, (data_local_sky_model, data_source_find)) =>
      var label: String = "Update LSM (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      for (data <- data_local_sky_model) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      for (data <- data_source_find) {
        hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        input_size += data.length
      }
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
 //     Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }

}
//  * 4194 tasks
//  * 361.64 GB produced (reduced from 36164.34 GB, factor 100)
//  * 764.82 Pflop represented
// This is roughly(!):
//  * 2055.98 min node time (6.20 Tflop/s effective)
//  * 2185.21 s island time (0.35 Pflop/s effective)
//  * 54.63 s cluster time (14.00 Pflop/s effective)
//  * 1.52% SKA SDP
//  * 0.0152% SKA SDP internal data rate
