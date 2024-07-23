package io.github.rkolesnev.lucenekstreams;

import ch.hsr.geohash.GeoHash;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeoHashId {

  String geohash;

  public GeoHashId(GeoHash geoHash){
    this.geohash = geoHash.toBase32();
  }
}
