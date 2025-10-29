import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { ToastProvider } from './components/ui/Toast'
import { ThemeProvider } from './context/ThemeContext'
import LandingPage from './pages/LandingPage'
import DashboardPage from './pages/DashboardPage'
import HelpPage from './pages/HelpPage'

function App() {
  return (
    <ThemeProvider>
      <ToastProvider>
        <Router>
          <Routes>
            <Route path="/" element={<LandingPage />} />
            <Route path="/dashboard/:providerUuid" element={<DashboardPage />} />
            <Route path="/help" element={<HelpPage />} />
          </Routes>
        </Router>
      </ToastProvider>
    </ThemeProvider>
  )
}

export default App


