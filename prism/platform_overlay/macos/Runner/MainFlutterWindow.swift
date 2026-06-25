import Cocoa
import FlutterMacOS

class MainFlutterWindow: NSWindow {
  override func awakeFromNib() {
    let flutterViewController = FlutterViewController()
    let windowFrame = self.frame
    self.contentViewController = flutterViewController
    self.setFrame(windowFrame, display: true)

    // Futuristic seamless chrome: drop the grey title bar and let Prism's gradient
    // flow all the way to the top, with the traffic lights floating over the app.
    self.titlebarAppearsTransparent = true
    self.titleVisibility = .hidden
    self.styleMask.insert(.fullSizeContentView)
    self.isMovableByWindowBackground = true
    self.backgroundColor = NSColor(
      calibratedRed: 0.102, green: 0.090, blue: 0.078, alpha: 1.0) // Palette.bg0

    RegisterGeneratedPlugins(registry: flutterViewController)

    super.awakeFromNib()
  }
}
