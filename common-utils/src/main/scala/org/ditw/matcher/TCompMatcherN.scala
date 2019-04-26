package org.ditw.matcher

trait TCompMatcherN extends TCompMatcher {
  protected val subMatchers:Iterable[TCompMatcher]

  protected val refTags:Set[String] = {
    subMatchers.flatMap(CompMatcherNs.refTagsFromMatcher)
      .toSet
  }
  override def getRefTags: Set[String] = refTags

}
