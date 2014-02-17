package org.apache.spark.api.java

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.{FutureAction, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.api.java.function.{Function => JFunction, VoidFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class JavaAsyncRDD[T](val rdd: RDD[T])(implicit val classTag: ClassTag[T]) extends
JavaRDDLike[T, JavaAsyncRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaAsyncRDD[T] = JavaAsyncRDD.fromRDD(rdd)

  // Common RDD functions

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaAsyncRDD[T] = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
   */
  def persist(newLevel: StorageLevel): JavaAsyncRDD[T] = wrapRDD(rdd.persist(newLevel))

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * This method blocks until all blocks are deleted.
   */
  def unpersist(): JavaAsyncRDD[T] = wrapRDD(rdd.unpersist())

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean): JavaAsyncRDD[T] = wrapRDD(rdd.unpersist(blocking))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaAsyncRDD[T] = wrapRDD(rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int): JavaAsyncRDD[T] = wrapRDD(rdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaAsyncRDD[T] =
    wrapRDD(rdd.filter((x => f(x).booleanValue())))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int): JavaAsyncRDD[T] = rdd.coalesce(numPartitions)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaAsyncRDD[T] =
    rdd.coalesce(numPartitions, shuffle)

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int): JavaAsyncRDD[T] = rdd.repartition(numPartitions)

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaAsyncRDD[T] =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaAsyncRDD[T]): JavaAsyncRDD[T] = wrapRDD(rdd.union(other.rdd))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: JavaAsyncRDD[T]): JavaAsyncRDD[T] = wrapRDD(rdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaAsyncRDD[T], numPartitions: Int): JavaAsyncRDD[T] =
    wrapRDD(rdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaAsyncRDD[T], p: Partitioner): JavaAsyncRDD[T] =
    wrapRDD(rdd.subtract(other, p))

  override def toString = rdd.toString

  // Async RDD Actions

  /**
   * Returns a future for counting the number of elements in the RDD.
   */
  def countAsync(): FutureAction[Long] = rdd.countAsync()

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreachAsync(f: VoidFunction[T]): FutureAction[Unit] = rdd.foreachAsync(f)

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: VoidFunction[java.util.Iterator[T]]): FutureAction[Unit] = {
    rdd.foreachPartitionAsync((x => f(asJavaIterator(x))))
  }

  /** Assign a name to this RDD */
  def setName(name: String): JavaAsyncRDD[T] = {
    rdd.setName(name)
    this
  }
}

object JavaAsyncRDD {

  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaAsyncRDD[T] = new JavaAsyncRDD[T](rdd)

  implicit def toRDD[T](rdd: JavaAsyncRDD[T]): RDD[T] = rdd.rdd
}