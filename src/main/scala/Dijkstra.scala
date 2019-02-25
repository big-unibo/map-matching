import scala.collection.mutable

class Dijkstra[Key] {

  type PathInfo = (Double, List[Key])
  type Path = List[Key]
  type MinHeap[PathInfo] = mutable.PriorityQueue[PathInfo]

  final def dijkstra(weightedGraph: Map[Key, List[(Double, Key)]],
                     start: Key,
                     dest: Key)(implicit ord: Ordering[PathInfo]): PathInfo =
    dijkstraHelper(weightedGraph, mutable.PriorityQueue((0.0, List(start))), dest)

  @annotation.tailrec
  private final def dijkstraHelper(weightedGraph: Map[Key, List[(Double, Key)]],
                                   fringe: MinHeap[PathInfo],
                                   dest: Key,
                                   visited: Set[Key] = Set.empty[Key])(implicit ord: Ordering[PathInfo]): PathInfo = {

    def updateFringe(frng: MinHeap[PathInfo], currentDist: Double, currentPath: Path): MinHeap[PathInfo] =
      (currentPath: @unchecked) match {
        case keys@key :: _ =>
          weightedGraph(key)
            .withFilter { case (_, k) => !visited.contains(k) }
            .map { case (d, k) => (currentDist + d, k :: keys) } // updated PathInfo's
            .foreach { p => frng.enqueue(p) }

          frng
      }

    if (fringe.isEmpty)
      (0, Nil)
    else {
      (fringe.dequeue(): @unchecked) match {
        case (dist, path@`dest` :: _) =>
          (dist, path.reverse)

        case (dist, path@key :: _) =>
          dijkstraHelper(weightedGraph, updateFringe(fringe, dist, path), dest, visited + key)
      }
    }
  }
}