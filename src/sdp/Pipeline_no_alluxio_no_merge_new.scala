
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
import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;
import alluxio.client.file.FileSystem
import alluxio.client.WriteType
import alluxio.client.ReadType
import alluxio.client.file.options.OpenFileOptions
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import Array._
import org.apache.spark.broadcast.Broadcast
object Pipeline_no_alluxio_no_merge_new {
class SDPPartitioner_pharo(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.split(',')(2).toInt
  }
}
class SDPPartitioner_facets(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.split(',')(3).toInt
  }
}
class A(val aa :Array[((Int, Int, Int, Int, Int, Int),Array[Byte])])
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
    extract_lsm.cache()
    var broadcast_lsm=sc.broadcast(extract_lsm.collect())
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
    val visibility_buffer: RDD[((Int,Int,Int,Int,Int), Data)] = {
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
 
    // === Reprojection Predict + IFFT +Degrid===
    
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
    telescope_data.cache()
    var broads_input_telescope_data = sc.broadcast(telescope_data.collect())
  //  broads_input_telescope_data.cache()
    val reppre_ifft : RDD[((Int,Int,Int,Int,Int,Int), Data)] = {
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
    reppre_ifft.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
     //printf(" the size of new rdd reppre_ifft" + reppre_ifft.count())

    // === Telescope Data ===
    var degrid: RDD[((Int,Int,Int,Int,Int,Int), Data)] = {
      reppre_ifft.flatMap(ix=>degrid_kernel(ix,broads_input_telescope_data,broadcast_lsm))
    }
    degrid.cache()
    val pharotpre_dft_sumvis: RDD[((Int, Int, Int, Int, Int),Data)] = {
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
      degrid.partitionBy(new SDPPartitioner_pharo(20)).mapPartitions(pharotpre_dft_sumvis_kernel)

    }
    pharotpre_dft_sumvis.cache()
   // printf(" the size of new rdd pharotpre_dft_sumvis" + pharotpre_dft_sumvis.count())
  //  pharotpre_dft_sumvis.cache()
    var broads_input0=sc.broadcast(pharotpre_dft_sumvis.collect())
    var broads_input1=sc.broadcast(visibility_buffer.collect())
   // printf(" the size of new rdd pharotpre_dft_sumvis" + pharotpre_dft_sumvis.count())

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
      sc.parallelize(initset).map(ix => timeslots_kernel(ix,broads_input0,broads_input1))

    }
    timeslots.cache()
  //  printf(" the size of new rdd timeslots" + timeslots.count())
    // === Solve ===
    val solve: RDD[((Int, Int, Int, Int, Int),Data)] = {
      val dep_timeslots = HashMap[(Int, Int, Int, Int, Int, Int), ListBuffer[(Int, Int, Int, Int, Int)]]()
      val beam = 0
      val major_loop = 0
      val frequency = 0
      for (time <- 0 until 6) {
        val polarisation = 0
        dep_timeslots.getOrElseUpdate(Tuple6(beam, major_loop, frequency, time, 0, polarisation), ListBuffer()) += Tuple5(beam, major_loop, frequency, time, polarisation)
      }
      timeslots.map(solve_kernel)
      
    }
  //  printf(" the size of new rdd solve" + solve.count())
    // === Correct + Subtract Visibility + Flag ===
    solve.cache()
    var broads_input2=sc.broadcast(solve.collect())
    val cor_subvis_flag: RDD[((Int, Int, Int, Int, Int, Int),Data)] = {
      val initset = ListBuffer[(Int, Int, Int, Int, Int, Int)]()
      val beam = 0
      for (frequency <- 0 until 20) {
        var time = 0
        val baseline = 0
        val polarisation = 0
        val major_loop = 0
        initset += Tuple6(beam, major_loop, frequency, time, baseline, polarisation)
      }
      sc.parallelize(initset).map(ix => cor_subvis_flag_kernel(ix,broads_input0,broads_input1,broads_input2))

    }
    cor_subvis_flag.cache()
  //  printf(" the size of new rdd cor_subvis_flag" + cor_subvis_flag.count())
    var broads_input = sc.broadcast(cor_subvis_flag.collect())
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
      sc.parallelize(initset).map(ix => grikerupd_pharot_grid_fft_rep_kernel(ix, broads_input_telescope_data,broads_input))
    }
   grikerupd_pharot_grid_fft_rep.cache()
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
   // printf(" the size of new rdd sum_facets" + sum_facets.count())
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
      sum_facets.partitionBy(new SDPPartitioner_facets(36)).mapPartitions(identify_component_kernel_partitions)
    }
    var broads_input_identify = sc.broadcast(identify_component.collect())

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
    source_find.cache()
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
      sum_facets.map(ix => subimacom_kernel(ix, broads_input_identify))
    }
    subimacom.cache()
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
    sc.stop()
  }
  def extract_lsm_kernel: ((Int, Int)) => ((Int, Int), Data) = {
    case ix =>
       val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      (ix, result)
  }

  def read_visibiity_kernel: ((Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int), Data) = {
    case ix =>
      var results = HashMap[Int, Data]()
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "node9");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      var temp = ix._2
      val path2 = new AlluxioURI("/visibility_buffer/" + temp)
      using(fs.openFile(path2, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in2 =>
        var buf = new Array[Byte](in2.remaining().toInt)
        var result = in2.read(buf)
        results.put(1, buf)
        //  in2.close
      }

      (ix, results.get(1).get)
  }

  def read_pharo_kernel: ((Int, Int, Int, Int, Int)) => ((Int, Int, Int, Int, Int), Data) = {
    case ix =>
      var results = HashMap[Int, Data]()
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "node9");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      var temp = ix._2
      val path = new AlluxioURI("/pharotpre_dft_sumvis/" + temp)
      using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in =>
        var buf = new Array[Byte](in.remaining().toInt)
        var result = in.read(buf)
        results.put(0, buf)
        //    in.close
      }

      (ix, results.get(0).get)
  }
  def local_sky_model_kernel: (Unit) => (Unit, Data) = {
    case ix =>
      var label: String = "Local Sky Model (0.0 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 0).toInt))
      ByteBuffer.wrap(result).putInt(0, hash)
      Thread.sleep((scale_compute * 0).toInt)
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
      Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try { f(resource) } finally { resource.close() }

  def visibility_buffer_kernel: ((Int, Int, Int, Int, Int)) => (((Int, Int, Int, Int, Int),Data)) = {
    case ix =>
      
      var label: String = "Visibility Buffer (36384.6 MB, 0.00 Tflop) " + ix.toString
      var hash: Int = MurmurHash3.stringHash(label)
      var input_size: Long = 0
      println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
      (ix,result)

  }

  def reppre_ifft_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int,Int,Int), Data)]],Broadcast[Array[((Int, Int), Data)]]) => ((Int, Int, Int, Int, Int, Int),Data) = {
    case (ix) =>
       val result_reffre = new Array[Byte](math.max(4, (scale_data * 329520000).toInt))
       (ix._1,result_reffre)
     
      
  }
 def degrid_kernel: (((Int, Int, Int, Int, Int, Int),Data), Broadcast[Array[((Int, Int,Int,Int), Data)]],Broadcast[Array[((Int, Int), Data)]]) => TraversableOnce[(((Int, Int, Int, Int, Int, Int),Data))] = {
    case (ix) =>
      var mylist = new Array[((Int, Int, Int, Int, Int, Int),Data)](4)
      val result2 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_2 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_3 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
      val result2_4 = new Array[Byte](math.max(4, (scale_data * 998572).toInt))
    
      var temp1 = ix._1._1._3 * 4
      mylist(0)=((ix._1._1._1,ix._1._1._2 ,temp1,ix._1._1._4,ix._1._1._5,ix._1._1._6),result2)
      var temp2 = ix._1._1._3 * 4+1 
      mylist(1)=((ix._1._1._1,ix._1._1._2 ,temp2,ix._1._1._4,ix._1._1._5,ix._1._1._6),result2_2)
      var temp3 = ix._1._1._3 * 4 + 2
      mylist(2)=((ix._1._1._1,ix._1._1._2 ,temp3,ix._1._1._4,ix._1._1._5,ix._1._1._6),result2_3)
      var temp4 = ix._1._1._3 * 4+3
      mylist(3)=((ix._1._1._1,ix._1._1._2 ,temp4,ix._1._1._4,ix._1._1._5,ix._1._1._6),result2_4)
      mylist
      
  }
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
      Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
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
      Thread.sleep((scale_compute * 23).toInt)
      (ix, result)
  }
  def pharotpre_dft_sumvis_kernel: (Iterator[((Int, Int, Int, Int, Int, Int), Data)]) => (Iterator[((Int, Int, Int, Int, Int),Data)]) = {
    case (ix) =>
      var label: String = "Phase Rotation Predict + DFT + Sum visibilities (36384.6 MB, 102.32 Tflop) " + ix.toString
      //read data from alluxio
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
       var result2 = new Array[((Int, Int, Int, Int,Int),Data)](1)
   
    if(ix.hasNext)
    {
         var temp=ix.next()
         var result2 = new Array[((Int, Int, Int, Int,Int),Data)](1)
         result2(0)=((temp._1._1,temp._1._2,temp._1._3,temp._1._4,temp._1._6),result)
         result2.iterator
         
     }
    else
    {
	      var result2 = new Array[((Int, Int, Int, Int,Int),Data)](1)
	      result2(0)=((0,0,0,0,0),result)
	      result2.iterator
      
    }
   
  }
  def pharotpre_dft_sumvis_kernel2: ((Int, Int, Int, Int, Int)) => (Int, Int, Int, Int, Int) = {
    case ix =>
      var label: String = "Phase Rotation Predict + DFT + Sum visibilities (36384.6 MB, 102.32 Tflop) " + ix.toString
      Configuration.set(PropertyKey.MASTER_HOSTNAME, "node9");
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      val fs = FileSystem.Factory.get();
      for (i_facet <- 0 until 36)
        for (polarization <- 0 until 4) {
          var temp = 0 + "_" + ix._2 + "_" + i_facet + "_" + polarization
          val path = new AlluxioURI("/degkerupd_deg/" + temp)
          using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))) { in =>
            var buf = new Array[Byte](in.remaining().toInt)
            var tempdata = in.read(buf)
          }
        } //for 
      val result = new Array[Byte](math.max(4, (scale_data * 363846353).toInt))
      //write the pair to alluxio
      val path3 = new AlluxioURI("/pharotpre_dft_sumvis/" + ix._2)
      using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE))) { out =>
        out.write(result)
      } //using
      (ix)
  }

  def timeslots_kernel: ((Int, Int, Int, Int, Int, Int), Broadcast[Array[((Int, Int, Int, Int, Int), Data)]], Broadcast[Array[((Int, Int, Int, Int, Int), Data)]]) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 15182856).toInt))
      (ix._1, result)
  }
 
  def solve_kernel: ((((Int, Int, Int, Int, Int,Int),Data))) => ((Int, Int, Int, Int, Int),Data) = {
    case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 82627788).toInt))
      var newix=new Tuple5(ix._1._1,ix._1._2,ix._1._3,ix._1._4,ix._1._6)
      (newix,result)
  }

  def cor_subvis_flag_kernel: ((Int, Int, Int, Int, Int,Int),Broadcast[Array[((Int, Int,Int,Int,Int), Data)]],Broadcast[Array[((Int, Int,Int,Int,Int), Data)]],Broadcast[Array[((Int, Int,Int,Int,Int), Data)]]) => ((Int, Int, Int, Int, Int, Int),Data) = {
    case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 367559071).toInt))
      (ix._1,result)
  }
   def grikerupd_pharot_grid_fft_rep_kernel: ((Int, Int, Int, Int, Int, Int),  Broadcast[Array[((Int, Int, Int, Int), Data)]],Broadcast[Array[((Int, Int, Int, Int,Int,Int), Data)]]) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case ix =>
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix._1, result)

  }
  def sum_facets_kernel: (((Int, Int, Int, Int, Int, Int), Data)) => ((Int, Int, Int, Int, Int, Int), Data) = {
    case (ix) =>
      val result = new Array[Byte](math.max(4, (scale_data * 329509848).toInt))
      (ix._1, result)
  }
  def identify_component_kernel: (((Int, Int, Int, Int), Tuple1[Iterable[Data]])) => ((Int, Int, Int, Int), Data) = {
    case (ix, Tuple1(data_sum_facets)) =>
      var label: String = "Identify Component (0.2 MB, 4118.87 Tflop) " + ix.toString
   //   var hash: Int = MurmurHash3.stringHash(label)
    //  var input_size: Long = 0
    //  for (data <- data_sum_facets) {
      //  hash ^= MurmurHash3.bytesHash(data.slice(0, 4))
        //input_size += data.length
      //}
     // println(label + " (hash " + Integer.toHexString(hash) + " from " + (input_size / 1000000).toString() + " MB input)")
      val result = new Array[Byte](math.max(4, (scale_data * 1600).toInt))
    //  ByteBuffer.wrap(result).putInt(0, hash)
    //  Thread.sleep((scale_compute * 664334).toInt)
      (ix, result)
  }
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
      Thread.sleep((scale_compute * 0).toInt)
      (ix, result)
  }

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
      Thread.sleep((scale_compute * 0).toInt)
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
