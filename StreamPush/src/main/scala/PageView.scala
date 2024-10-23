import java.sql.{Date, Timestamp}

class PageView (private var timeCreate : Timestamp,
                private var cookieCreate : Timestamp,
                private var browserCode : Int,
                private var browserVer : String,
                private var osCode : Int,
                private var osVer : String,
                private var ip : Long,
                private var locId : Int,
                private var domain : String,
                private var siteId : Int,
                private var cId : Int,
                private var path : String,
                private var referer : String,
                private var guid : Long,
                private var flashVersion : String,
                private var jre : String,
                private var sr : String,
                private var sc : String,
                private var geographic : Int,
                private var category : String){
	//    timeCreate = 0
	//    cookieCreate = 1
	//    browserCode = 2
	//    browserVer = 3
	//    osCode = 4
	//    osVer = 5
	//    ip = 6 (kieu long)
	//    locId = 7
	//    domain = 8
	//    siteId = 9
	//    cId = 10
	//    path = 11
	//    referer = 12
	//    guid = 13
	//    flashVersion = 14
	//    jre = 15
	//    sr = 16
	//    sc = 17
	//    geographic = 18
	//    category = 23
	def getTimeCreate : Timestamp = timeCreate
	
	def getCookieCreate : Timestamp = cookieCreate
	
	def getBrowserCode : Int = browserCode
	
	def getBrowserVer : String = browserVer
	
	def getOsCode : Int = osCode
	
	def getOsVer : String = osVer
	
	def getIp : Long = ip
	
	def getLocId : Int = locId
	
	def getDomain : String = domain
	
	def getSiteId : Int = siteId
	
	def getcId : Int = cId
	
	def getPath : String = path
	
	def getReferer : String = referer
	
	def getGuid : Long = guid
	
	def getFlashVersion : String = flashVersion
	
	def getJre : String = jre
	
	def getSr : String = sr
	
	def getSc : String = sc
	
	def getGeographic : Int = geographic
	
	def getCategory : String = category
	
	def setTimeCreate (timeCreate : Timestamp) : Unit ={
		this.timeCreate = timeCreate
	}
	
	def setCookieCreate (cookieCreate : Timestamp) : Unit ={
		this.cookieCreate = cookieCreate
	}
	
	def setBrowserCode (browserCode : Int) : Unit ={
		this.browserCode = browserCode
	}
	
	def setBrowserVer (browserVer : String) : Unit ={
		this.browserVer = browserVer
	}
	
	def setOsCode (osCode : Int) : Unit ={
		this.osCode = osCode
	}
	
	def setOsVer (osVer : String) : Unit ={
		this.osVer = osVer
	}
	
	def setIp (ip : Long) : Unit ={
		this.ip = ip
	}
	
	def setLocId (locId : Int) : Unit ={
		this.locId = locId
	}
	
	def setDomain (domain : String) : Unit ={
		this.domain = domain
	}
	
	def setSiteId (siteId : Int) : Unit ={
		this.siteId = siteId
	}
	
	def setcId (cId : Int) : Unit ={
		this.cId = cId
	}
	
	def setPath (path : String) : Unit ={
		this.path = path
	}
	
	def setReferer (referer : String) : Unit ={
		this.referer = referer
	}
	
	def setGuid (guid : Long) : Unit ={
		this.guid = guid
	}
	
	def setFlashVersion (flashVersion : String) : Unit ={
		this.flashVersion = flashVersion
	}
	
	def setJre (jre : String) : Unit ={
		this.jre = jre
	}
	
	def setSr (sr : String) : Unit ={
		this.sr = sr
	}
	
	def setSc (sc : String) : Unit ={
		this.sc = sc
	}
	
	def setGeographic (geographic : Int) : Unit ={
		this.geographic = geographic
	}
	
	def setCategory (category : String) : Unit ={
		this.category = category
	}
	
	
	private def canEqual (other : Any) : Boolean = other.isInstanceOf[PageView]
	
	override def equals (other : Any) : Boolean = other match {
		case that : PageView =>
			that.canEqual(this) &&
					timeCreate == that.timeCreate &&
					cookieCreate == that.cookieCreate &&
					browserCode == that.browserCode &&
					browserVer == that.browserVer &&
					osCode == that.osCode &&
					osVer == that.osVer &&
					ip == that.ip &&
					locId == that.locId &&
					domain == that.domain &&
					siteId == that.siteId &&
					cId == that.cId &&
					path == that.path &&
					referer == that.referer &&
					guid == that.guid &&
					flashVersion == that.flashVersion &&
					jre == that.jre &&
					sr == that.sr &&
					sc == that.sc &&
					geographic == that.geographic &&
					category == that.category
		case _ => false
	}
	
	override def hashCode () : Int ={
		val state = Seq(timeCreate, cookieCreate, browserCode, browserVer, osCode, osVer, ip, locId, domain, siteId, cId, path, referer, guid, flashVersion, jre, sr, sc, geographic, category)
		state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
	}
	
	override def toString = s"PageView($timeCreate, $cookieCreate, $browserCode, $browserVer, $osCode, $osVer, $ip, $locId, $domain, $siteId, $cId, $path, $referer, $guid, $flashVersion, $jre, $sr, $sc, $geographic, $category)"
}
